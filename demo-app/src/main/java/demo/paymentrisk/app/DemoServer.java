package demo.paymentrisk.app;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.app.health.HealthMonitor;
import demo.paymentrisk.app.infra.FlinkRest;
import demo.paymentrisk.app.infra.FlussGateway;
import demo.paymentrisk.app.infra.KafkaGateway;
import demo.paymentrisk.app.infra.PythonRunnerClient;
import demo.paymentrisk.app.infra.SqlGatewayClient;
import demo.paymentrisk.app.playground.EventsWorkspace;
import demo.paymentrisk.app.playground.PythonWorkspace;
import demo.paymentrisk.app.playground.SqlPolicy;
import demo.paymentrisk.app.playground.SqlWorkspace;
import demo.paymentrisk.app.run.ExecutionHistory;
import demo.paymentrisk.app.run.Run;
import demo.paymentrisk.app.run.RunManager;
import demo.paymentrisk.app.scenario.Scenario;
import demo.paymentrisk.app.scenario.ScenarioCatalog;
import demo.paymentrisk.app.web.SseHub;
import demo.paymentrisk.app.web.WebServer;
import demo.paymentrisk.common.ComputedResults;
import demo.paymentrisk.common.Json;
import demo.paymentrisk.common.TableSchemas;
import demo.paymentrisk.common.rule.RuleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wires the demo app: health monitor, stage orchestration, workspaces, the snapshot publisher
 * (SSE) and the web server. On startup it creates a run (identifiers only); it never emits
 * events or starts jobs on its own.
 */
public final class DemoServer {
    private static final Logger LOG = LoggerFactory.getLogger(DemoServer.class);

    private final AppConfig cfg;
    private final HealthMonitor health;
    private final RunManager runs;
    private final EventsWorkspace events;
    private final SqlWorkspace sql;
    private final PythonWorkspace python;
    private final ExecutionHistory history;
    private final SseHub sse;
    private final AtomicBoolean dirty = new AtomicBoolean(true);

    private DemoServer(AppConfig cfg) throws Exception {
        this.cfg = cfg;
        Scenario scenario = new ScenarioCatalog(cfg.scenariosDir).get(cfg.scenarioId);
        if (scenario == null) {
            throw new IllegalStateException("scenario " + cfg.scenarioId + " not found in " + cfg.scenariosDir);
        }
        KafkaGateway kafka = new KafkaGateway(cfg.kafkaBootstrap);
        FlussGateway fluss = new FlussGateway(cfg.flussBootstrap, cfg.flussDatabase);
        FlinkRest flink = new FlinkRest(cfg.flinkRestUrl);
        SqlGatewayClient gateway = new SqlGatewayClient(cfg.sqlGatewayUrl);
        PythonRunnerClient runner = new PythonRunnerClient(cfg.pythonRunnerUrl);
        this.history = new ExecutionHistory(this::markDirty);
        this.health = new HealthMonitor(cfg, kafka, fluss, flink, gateway, runner, this::markDirty);
        this.runs = new RunManager(cfg, kafka, fluss, flink, scenario, history, this::markDirty);
        this.events = new EventsWorkspace(cfg, kafka, runs, history, this::markDirty);
        this.sql = new SqlWorkspace(cfg, gateway, flink, kafka, runs, history, this::markDirty);
        this.python = new PythonWorkspace(cfg, runner, runs, history, this::markDirty);
        this.sse = new SseHub(this::snapshot);
        runs.newRun();
        new WebServer(cfg, runs, events, sql, python, fluss, sse, this::snapshot).start();
        health.start();
        Thread pub = new Thread(this::publishLoop, "snapshot-publisher");
        pub.setDaemon(true);
        pub.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafka.close();
            fluss.close();
        }));
    }

    public static void start(AppConfig cfg) throws Exception {
        new DemoServer(cfg);
    }

    private void markDirty() {
        dirty.set(true);
    }

    private void publishLoop() {
        String last = null;
        long lastBeat = System.currentTimeMillis();
        while (true) {
            try {
                Thread.sleep(100);
                if (dirty.getAndSet(false)) {
                    String body = Json.write(snapshotBody());
                    if (!body.equals(last)) {
                        last = body;
                        sse.broadcast(snapshot());
                        lastBeat = System.currentTimeMillis();
                    }
                }
                if (System.currentTimeMillis() - lastBeat > 15_000) {
                    sse.heartbeat();
                    lastBeat = System.currentTimeMillis();
                }
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                LOG.warn("publish failed: {}", e.toString());
            }
        }
    }

    private String snapshot() {
        ObjectNode s = snapshotBody();
        s.put("server_time", Instant.now().toString());
        s.put("sse_clients", sse.clientCount());
        return Json.write(s);
    }

    private ObjectNode snapshotBody() {
        ObjectNode s = Json.mapper().createObjectNode();
        ObjectNode app = s.putObject("app");
        app.put("ui_version", cfg.uiVersion);
        app.put("input_topic", cfg.inputTopic);
        app.put("computed_topic", cfg.computedTopic);
        app.put("playground_topic", cfg.playgroundTopic);
        app.put("fluss_database", cfg.flussDatabase);
        app.put("flink_ui_url", cfg.flinkUiPublicUrl);
        app.put("computed_contract", ComputedResults.CONTRACT);
        RuleConfig rule = new RuleConfig(cfg.ruleThresholdMinor, RuleConfig.DEFAULT_CURRENCY, RuleConfig.DEFAULT_WINDOW);
        app.put("rule", rule.describe());
        app.put("rule_version", RuleConfig.RULE_VERSION);
        app.put("rule_threshold_minor", cfg.ruleThresholdMinor);
        app.put("rule_window_minutes", RuleConfig.DEFAULT_WINDOW.toMinutes());
        app.put("read_method", FlussGateway.READ_METHOD);
        app.put("sql_engine", SqlWorkspace.ENGINE);
        app.put("python_client", PythonWorkspace.CLIENT);
        app.put("python_enabled", cfg.pythonEnabled);
        app.put("auth_required", !cfg.accessToken.isEmpty());
        app.put("busy_operation", runs.busyOperation());
        app.put("runs_created", runs.runsCreated());
        ObjectNode limits = app.putObject("limits");
        limits.put("sql_max_rows", cfg.sqlMaxRows);
        limits.put("sql_batch_timeout_s", cfg.sqlBatchTimeoutMs / 1000);
        limits.put("sql_streaming_max_s", cfg.sqlStreamingMaxMs / 1000);
        limits.put("sql_max_concurrent", cfg.sqlMaxConcurrent);
        limits.put("sql_max_length", SqlPolicy.MAX_LENGTH);
        limits.put("python_timeout_s", cfg.pythonTimeoutSeconds);
        limits.put("stage_job_timeout_s", cfg.stageJobTimeoutMs / 1000);
        limits.put("batch_max_records", EventsWorkspace.MAX_BATCH);
        ArrayNode tables = app.putArray("tables");
        for (TableSchemas.TableSpec t : TableSchemas.all()) {
            ObjectNode o = tables.addObject();
            o.put("name", t.name);
            o.put("comment", t.comment);
            o.set("primary_key", Json.mapper().valueToTree(t.primaryKey));
            ArrayNode cols = o.putArray("columns");
            for (TableSchemas.Column c : t.columns) {
                cols.addObject().put("name", c.name).put("type", c.type == TableSchemas.ColumnType.TIMESTAMP_LTZ_3
                        ? "TIMESTAMP_LTZ(3)" : c.type.name()).put("comment", c.comment);
            }
        }
        s.set("health", health.toJson());
        Run run = runs.current();
        if (run != null) {
            runs.computeGates(run);
        }
        s.set("run", run == null ? null : Json.mapper().valueToTree(run));
        s.set("recent_events", Json.mapper().valueToTree(events.recent()));
        // Rows and raw responses only for the latest execution of each workspace tab (keeps snapshots small).
        ArrayNode sqlx = Json.mapper().valueToTree(sql.executions());
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (com.fasterxml.jackson.databind.JsonNode x : sqlx) {
            if (!seen.add(x.path("workspace").asText())) {
                ((ObjectNode) x).remove(java.util.List.of("rows", "raw_last_response"));
                ((ObjectNode) x).put("rows_omitted", true);
            }
        }
        s.set("sql_executions", sqlx);
        s.set("python_executions", Json.mapper().valueToTree(python.executions()));
        s.set("history", Json.mapper().valueToTree(history.list()));
        return s;
    }
}
