package demo.paymentrisk.app.playground;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.app.AppConfig;
import demo.paymentrisk.app.infra.FlinkRest;
import demo.paymentrisk.app.infra.KafkaGateway;
import demo.paymentrisk.app.infra.SqlGatewayClient;
import demo.paymentrisk.app.run.ExecutionHistory;
import demo.paymentrisk.app.run.Run;
import demo.paymentrisk.app.run.RunManager;
import demo.paymentrisk.common.ComputedResults;
import demo.paymentrisk.common.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Executes SQL through the Flink SQL Gateway against the existing Flink session cluster. Each
 * execution gets its own gateway session with the demo's tables predefined. Results, job IDs,
 * states and errors are whatever Flink reports; nothing is computed here.
 */
public final class SqlWorkspace {
    private static final Logger LOG = LoggerFactory.getLogger(SqlWorkspace.class);
    private static final int MAX_EXECUTIONS = 30;
    private static final Set<String> TERMINAL = Set.of("FINISHED", "FAILED", "CANCELED");
    public static final String ENGINE = "Apache Flink 1.20 SQL, executed by the Flink SQL Gateway (REST API v2) "
            + "on the demo's Flink session cluster";

    public static final class Conflict extends Exception {
        public Conflict(String m) {
            super(m);
        }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Output {
        public String topic;
        public Long start;
        public Long end;
        public int records;
        public int valid;
        public int invalid;
        public Map<String, Integer> runIds = new LinkedHashMap<>();
        public List<String> firstErrors = new ArrayList<>();
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Execution {
        public String id;
        /** "sql" (SQL Workspace) or "query" (Query Workspace, SQL tab) */
        public String workspace;
        public String statement;
        /** batch | streaming */
        public String mode;
        /** QUERY | INSERT | EXPLAIN | SHOW | DESCRIBE */
        public String kind;
        public String engine = ENGINE;
        /** SUBMITTING | RUNNING | FINISHED | FAILED | CANCELED */
        public volatile String status = "SUBMITTING";
        public volatile String phase;
        public volatile String jobId;
        public volatile String jobUrl;
        public volatile String jobState;
        public volatile List<ObjectNode> columns = new ArrayList<>();
        public final List<JsonNode> rows = new CopyOnWriteArrayList<>();
        public volatile int rowCount;
        public volatile boolean truncated;
        public volatile String note;
        public String startedAt = Instant.now().toString();
        public volatile String finishedAt;
        public volatile Long durationMs;
        public volatile String error;
        public volatile String errorDetail;
        public volatile JsonNode rawLastResponse;
        public volatile Output output;
        transient volatile boolean cancelRequested;
    }

    private final AppConfig cfg;
    private final SqlGatewayClient gateway;
    private final FlinkRest flink;
    private final KafkaGateway kafka;
    private final RunManager runs;
    private final ExecutionHistory history;
    private final Runnable onChange;
    private final List<Execution> executions = new CopyOnWriteArrayList<>();

    public SqlWorkspace(AppConfig cfg, SqlGatewayClient gateway, FlinkRest flink, KafkaGateway kafka, RunManager runs,
                        ExecutionHistory history, Runnable onChange) {
        this.cfg = cfg;
        this.gateway = gateway;
        this.flink = flink;
        this.kafka = kafka;
        this.runs = runs;
        this.history = history;
        this.onChange = onChange;
    }

    public List<Execution> executions() {
        return new ArrayList<>(executions);
    }

    public synchronized Execution submit(String statement, String mode, String workspace) throws SqlPolicy.Rejected, Conflict {
        String m = "streaming".equalsIgnoreCase(mode) ? "streaming" : "batch";
        String ws = "query".equals(workspace) ? "query" : "sql";
        String kind = SqlPolicy.check(statement);
        long running = executions.stream().filter(x -> !isDone(x)).count();
        for (Execution x : executions) {
            if (!isDone(x) && x.workspace.equals(ws)) {
                throw new Conflict("A statement is already running in this workspace (" + x.id + "). Cancel it or wait for it to finish.");
            }
        }
        if (running >= cfg.sqlMaxConcurrent) {
            throw new Conflict("At most " + cfg.sqlMaxConcurrent + " SQL statements can run at once. Cancel one first.");
        }
        Execution x = new Execution();
        x.id = "sql-" + UUID.randomUUID().toString().substring(0, 8);
        x.workspace = ws;
        x.statement = statement.trim();
        x.mode = m;
        x.kind = kind;
        executions.add(0, x);
        while (executions.size() > MAX_EXECUTIONS) {
            Execution old = executions.get(executions.size() - 1);
            if (!isDone(old)) {
                break;
            }
            executions.remove(executions.size() - 1);
        }
        Run r = runs.current();
        ExecutionHistory.Entry h = history.add(x.id, "SQL_STATEMENT", kind + " (" + m + ", "
                + ("query".equals(ws) ? "Query Workspace" : "SQL Workspace") + "): " + abbreviate(x.statement, 90),
                r == null ? null : r.runId);
        Thread t = new Thread(() -> execute(x, h), "sql-" + x.id);
        t.setDaemon(true);
        t.start();
        onChange.run();
        return x;
    }

    public boolean cancel(String id) {
        for (Execution x : executions) {
            if (x.id.equals(id) && !isDone(x)) {
                x.cancelRequested = true;
                x.phase = "Cancelling";
                onChange.run();
                return true;
            }
        }
        return false;
    }

    private static boolean isDone(Execution x) {
        return "FINISHED".equals(x.status) || "FAILED".equals(x.status) || "CANCELED".equals(x.status);
    }

    // ------------------------------------------------------------------ execution

    private void execute(Execution x, ExecutionHistory.Entry h) {
        String session = null;
        String op = null;
        long started = System.currentTimeMillis();
        try {
            x.phase = "Opening a SQL Gateway session";
            onChange.run();
            Map<String, String> props = new LinkedHashMap<>();
            props.put("execution.runtime-mode", x.mode);
            props.put("restart-strategy.type", "none");
            props.put("pipeline.name", "sql-workspace " + x.id);
            session = gateway.openSession("prsd-" + x.id, props);
            for (String ddl : sessionDdl()) {
                gateway.executeDdl(session, ddl);
            }
            if ("INSERT".equals(x.kind)) {
                x.output = new Output();
                x.output.topic = cfg.computedTopic;
                x.output.start = kafka.endOffset(cfg.computedTopic, 0, Duration.ofSeconds(8));
            }
            x.phase = "Submitting the statement";
            onChange.run();
            op = gateway.submit(session, x.statement, null);
            x.status = "RUNNING";
            x.phase = "Running";
            onChange.run();
            if ("INSERT".equals(x.kind)) {
                runInsert(x, session, op, started);
            } else {
                runQuery(x, session, op, started);
            }
        } catch (SqlGatewayClient.SqlError e) {
            x.error = e.summary;
            x.errorDetail = e.detail;
            x.status = "FAILED";
        } catch (Exception e) {
            x.error = RunManager.root(e);
            x.status = "FAILED";
            LOG.warn("SQL execution {} failed: {}", x.id, x.error);
        } finally {
            if (session != null) {
                if (op != null) {
                    gateway.closeOperation(session, op);
                }
                gateway.closeSession(session);
            }
            if (x.jobId != null) {
                x.jobState = jobState(x.jobId);
                x.jobUrl = cfg.flinkUiPublicUrl + "/#/job/" + (TERMINAL.contains(String.valueOf(x.jobState)) ? "completed" : "running")
                        + "/" + x.jobId + "/overview";
                if ("FAILED".equals(x.jobState) && x.error == null) {
                    x.error = flink.rootCause(x.jobId);
                    x.status = "FAILED";
                }
            }
            if ("RUNNING".equals(x.status) || "SUBMITTING".equals(x.status)) {
                x.status = "FINISHED";
            }
            x.phase = null;
            x.finishedAt = Instant.now().toString();
            x.durationMs = System.currentTimeMillis() - started;
            h.jobId = x.jobId;
            h.jobUrl = x.jobUrl;
            h.finish("FINISHED".equals(x.status) ? "SUCCEEDED" : x.status,
                    x.error != null ? x.error : ("INSERT".equals(x.kind) && x.output != null
                            ? x.output.records + " record(s) written to " + x.output.topic
                            : x.rowCount + " row(s)" + (x.truncated ? " (limit reached)" : "")) + (x.note == null ? "" : "; " + x.note));
            onChange.run();
        }
    }

    private void runQuery(Execution x, String session, String op, long started) throws Exception {
        boolean streaming = "streaming".equals(x.mode);
        long limitMs = streaming ? cfg.sqlStreamingMaxMs : cfg.sqlBatchTimeoutMs;
        long token = 0;
        while (true) {
            if (x.cancelRequested) {
                cancelRunning(x, session, op);
                x.status = "CANCELED";
                x.note = "Cancelled by the user after " + x.rowCount + " row(s).";
                return;
            }
            if (System.currentTimeMillis() - started > limitMs) {
                cancelRunning(x, session, op);
                if (streaming) {
                    x.status = "CANCELED";
                    x.note = "Streaming query stopped after the " + limitMs / 1000 + " s limit.";
                } else {
                    x.status = "FAILED";
                    x.error = "The statement did not finish within " + limitMs / 1000 + " s and was cancelled.";
                }
                return;
            }
            JsonNode r = gateway.fetch(session, op, token);
            x.rawLastResponse = trimRaw(r);
            if (x.jobId == null && r.hasNonNull("jobID")) {
                x.jobId = r.get("jobID").asText();
                x.jobUrl = cfg.flinkUiPublicUrl + "/#/job/running/" + x.jobId + "/overview";
            }
            JsonNode res = r.path("results");
            if (res.path("columns").size() > 0 && x.columns.isEmpty()) {
                List<ObjectNode> cols = new ArrayList<>();
                for (JsonNode c : res.path("columns")) {
                    ObjectNode o = Json.mapper().createObjectNode();
                    o.put("name", c.path("name").asText());
                    o.put("type", c.path("logicalType").path("type").asText());
                    cols.add(o);
                }
                x.columns = cols;
            }
            boolean grew = false;
            for (JsonNode d : res.path("data")) {
                x.rowCount++;
                if (x.rows.size() < cfg.sqlMaxRows) {
                    ObjectNode row = Json.mapper().createObjectNode();
                    row.put("kind", d.path("kind").asText("INSERT"));
                    row.set("fields", d.path("fields"));
                    x.rows.add(row);
                    grew = true;
                }
            }
            if (grew) {
                onChange.run();
            }
            if (x.rows.size() >= cfg.sqlMaxRows) {
                x.truncated = true;
                x.note = "Stopped after " + cfg.sqlMaxRows + " rows (result limit).";
                cancelRunning(x, session, op);
                return;
            }
            String type = r.path("resultType").asText();
            if ("EOS".equals(type) || !r.hasNonNull("nextResultUri")) {
                return;
            }
            String next = r.get("nextResultUri").asText();
            token = Long.parseLong(next.replaceAll(".*/result/(\\d+).*", "$1"));
            if ("NOT_READY".equals(type) || res.path("data").size() == 0) {
                Thread.sleep(200);
            }
        }
    }

    private void runInsert(Execution x, String session, String op, long started) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (x.jobId == null) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("The SQL Gateway did not report a job ID within 30 s.");
            }
            JsonNode r = gateway.fetch(session, op, 0);
            x.rawLastResponse = trimRaw(r);
            if (r.hasNonNull("jobID")) {
                x.jobId = r.get("jobID").asText();
            } else {
                for (JsonNode d : r.path("results").path("data")) {
                    x.jobId = d.path("fields").path(0).asText(null);
                }
            }
            if (x.jobId == null) {
                Thread.sleep(200);
            }
        }
        x.jobUrl = cfg.flinkUiPublicUrl + "/#/job/running/" + x.jobId + "/overview";
        x.phase = "INSERT job submitted; waiting for Flink";
        onChange.run();
        boolean streaming = "streaming".equals(x.mode);
        long limitMs = streaming ? cfg.sqlStreamingMaxMs : cfg.sqlBatchTimeoutMs;
        while (true) {
            String state = jobState(x.jobId);
            if (state != null && !state.equals(x.jobState)) {
                x.jobState = state;
                x.phase = "Flink job " + state;
                onChange.run();
            }
            if (TERMINAL.contains(String.valueOf(state))) {
                if ("FAILED".equals(state)) {
                    x.status = "FAILED";
                    x.error = flink.rootCause(x.jobId);
                } else if ("CANCELED".equals(state)) {
                    x.status = "CANCELED";
                }
                break;
            }
            if (x.cancelRequested) {
                cancelRunning(x, session, op);
                x.status = "CANCELED";
                x.note = "Cancelled by the user.";
                break;
            }
            if (System.currentTimeMillis() - started > limitMs) {
                cancelRunning(x, session, op);
                if (streaming) {
                    x.status = "CANCELED";
                    x.note = "Streaming INSERT stopped after the " + limitMs / 1000 + " s limit.";
                } else {
                    x.status = "FAILED";
                    x.error = "The INSERT job did not finish within " + limitMs / 1000 + " s and was cancelled.";
                }
                break;
            }
            Thread.sleep(500);
        }
        summarizeOutput(x);
    }

    /** Reads what the INSERT wrote to computed-results and checks it against the ingestion contract. */
    private void summarizeOutput(Execution x) {
        Output o = x.output;
        try {
            o.end = kafka.endOffset(cfg.computedTopic, 0, Duration.ofSeconds(8));
            for (KafkaGateway.Rec rec : kafka.read(cfg.computedTopic, 0, o.start, o.end, 5_000, Duration.ofSeconds(15))) {
                ComputedResults.Parsed p = ComputedResults.parse(rec.value);
                o.records++;
                o.runIds.merge(String.valueOf(p.runId), 1, Integer::sum);
                if (p.ok()) {
                    o.valid++;
                } else {
                    o.invalid++;
                    if (o.firstErrors.size() < 5) {
                        o.firstErrors.add("offset " + rec.offset + ": " + String.join("; ", p.errors));
                    }
                }
            }
            if (o.invalid > 0) {
                x.note = o.invalid + " of " + o.records + " record(s) do not satisfy the computed-result/v1 contract; "
                        + "the ingestion job will reject them.";
            }
        } catch (Exception e) {
            x.note = "Could not read back the written records: " + RunManager.root(e);
        }
    }

    private void cancelRunning(Execution x, String session, String op) {
        gateway.cancel(session, op);
        if (x.jobId != null) {
            try {
                flink.cancel(x.jobId);
            } catch (Exception ignored) {
                // may already be finished
            }
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                String s = jobState(x.jobId);
                x.jobState = s;
                if (TERMINAL.contains(String.valueOf(s))) {
                    break;
                }
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private String jobState(String jobId) {
        try {
            return flink.job(jobId).path("state").asText(null);
        } catch (Exception e) {
            return null;
        }
    }

    private static JsonNode trimRaw(JsonNode r) {
        ObjectNode copy = r.deepCopy();
        JsonNode data = copy.path("results").path("data");
        if (data.isArray() && data.size() > 20) {
            ArrayNode a = Json.mapper().createArrayNode();
            for (int i = 0; i < 20; i++) {
                a.add(data.get(i));
            }
            ((ObjectNode) copy.path("results")).set("data", a);
            ((ObjectNode) copy.path("results")).put("data_truncated_for_display", data.size());
        }
        return copy;
    }

    private static String abbreviate(String s, int n) {
        String one = s.replaceAll("\\s+", " ");
        return one.length() > n ? one.substring(0, n) + "…" : one;
    }

    // ------------------------------------------------------------------ tables + examples

    /** The predefined tables of every session. */
    public List<String> sessionDdl() {
        String kafka = "'properties.bootstrap.servers' = '" + cfg.kafkaBootstrap + "',\n"
                + "  'properties.group.id' = 'sql-workspace',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n"
                + "  'scan.bounded.mode' = 'latest-offset'";
        return List.of(
                "CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = '" + cfg.flussBootstrap + "')",
                "CREATE TABLE business_events (\n"
                        + "  `schema_version` STRING,\n  `run_id` STRING,\n  `event_id` STRING,\n  `event_type` STRING,\n"
                        + "  `supplier_id` STRING,\n  `occurred_at` TIMESTAMP_LTZ(3),\n  `emitted_at` TIMESTAMP_LTZ(3),\n"
                        + "  `correlation_id` STRING,\n  `payload` STRING,\n"
                        + "  `kafka_partition` INT METADATA FROM 'partition' VIRTUAL,\n"
                        + "  `kafka_offset` BIGINT METADATA FROM 'offset' VIRTUAL,\n"
                        + "  `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL\n"
                        + ") COMMENT 'Kafka topic " + cfg.inputTopic + ": validated business events (JSON). payload is JSON text; use JSON_VALUE.'\n"
                        + "WITH (\n  'connector' = 'kafka',\n  'topic' = '" + cfg.inputTopic + "',\n  " + kafka + ",\n"
                        + "  'format' = 'json',\n  'json.timestamp-format.standard' = 'ISO-8601',\n  'json.ignore-parse-errors' = 'true'\n)",
                "CREATE TABLE playground_events (\n"
                        + "  `message_key` STRING,\n  `message` STRING,\n"
                        + "  `kafka_offset` BIGINT METADATA FROM 'offset' VIRTUAL,\n"
                        + "  `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL\n"
                        + ") COMMENT 'Kafka topic " + cfg.playgroundTopic + ": free-form JSON records; use JSON_VALUE(message, ...).'\n"
                        + "WITH (\n  'connector' = 'kafka',\n  'topic' = '" + cfg.playgroundTopic + "',\n  " + kafka + ",\n"
                        + "  'key.format' = 'raw',\n  'key.fields' = 'message_key',\n  'value.format' = 'raw',\n"
                        + "  'value.fields-include' = 'EXCEPT_KEY'\n)",
                "CREATE TABLE computed_results (\n"
                        + "  `contract` STRING,\n  `run_id` STRING,\n  `target_table` STRING,\n  `execution_id` STRING,\n"
                        + "  `producer` STRING,\n  `row_json` STRING,\n"
                        + "  `kafka_offset` BIGINT METADATA FROM 'offset' VIRTUAL,\n"
                        + "  `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL\n"
                        + ") COMMENT 'Kafka topic " + cfg.computedTopic + ": computed-result/v1 records, the input of the Fluss ingestion job.'\n"
                        + "WITH (\n  'connector' = 'kafka',\n  'topic' = '" + cfg.computedTopic + "',\n  " + kafka + ",\n"
                        + "  'format' = 'json',\n  'json.ignore-parse-errors' = 'true',\n  'sink.delivery-guarantee' = 'at-least-once'\n)");
    }

    /** Working examples for the current run. */
    public ArrayNode examples() {
        Run r = runs.current();
        String run = r == null ? "run-…" : r.runId;
        String sup = r == null ? "SUP-NORTHSTAR" : r.supplierId;
        String pay = r == null ? "PAY-NS-0001" : r.paymentId;
        String db = "fluss_catalog." + cfg.flussDatabase;
        ArrayNode out = Json.mapper().createArrayNode();
        ex(out, "sql", "events-of-run", "Read this run's business events", "batch", "QUERY",
                "SELECT kafka_offset, event_type, event_id, occurred_at, supplier_id, payload\n"
                        + "FROM business_events\nWHERE run_id = '" + run + "'\nORDER BY kafka_offset");
        ex(out, "sql", "large-payments", "Filter: payment requests above USD 50,000", "batch", "QUERY",
                "SELECT run_id, event_id,\n  JSON_VALUE(payload, '$.payment_id') AS payment_id,\n"
                        + "  CAST(JSON_VALUE(payload, '$.amount_minor') AS BIGINT) / 100 AS amount_usd,\n"
                        + "  JSON_VALUE(payload, '$.beneficiary_masked') AS beneficiary\n"
                        + "FROM business_events\nWHERE event_type = 'PAYMENT_REQUESTED'\n"
                        + "  AND CAST(JSON_VALUE(payload, '$.amount_minor') AS BIGINT) > 5000000");
        ex(out, "sql", "bank-change-fields", "Select fields: beneficiary changes of this run", "batch", "QUERY",
                "SELECT occurred_at,\n  JSON_VALUE(payload, '$.change_id') AS change_id,\n"
                        + "  JSON_VALUE(payload, '$.actor_id') AS actor_id,\n  JSON_VALUE(payload, '$.session_id') AS session_id,\n"
                        + "  JSON_VALUE(payload, '$.before.beneficiary_masked') AS before_account,\n"
                        + "  JSON_VALUE(payload, '$.after.beneficiary_masked') AS after_account\n"
                        + "FROM business_events\nWHERE run_id = '" + run + "' AND event_type = 'BENEFICIARY_CHANGED'");
        ex(out, "sql", "events-per-type", "Aggregate: events per type for this run", "batch", "QUERY",
                "SELECT event_type, COUNT(*) AS events, MIN(occurred_at) AS first_at, MAX(occurred_at) AS last_at\n"
                        + "FROM business_events\nWHERE run_id = '" + run + "'\nGROUP BY event_type");
        ex(out, "sql", "playground-totals", "Aggregate: custom events by category", "batch", "QUERY",
                "SELECT JSON_VALUE(message, '$.category') AS category,\n  COUNT(*) AS records,\n"
                        + "  SUM(CAST(JSON_VALUE(message, '$.amount') AS DOUBLE)) AS total_amount\n"
                        + "FROM playground_events\nGROUP BY JSON_VALUE(message, '$.category')");
        ex(out, "sql", "playground-filter", "Filter: custom events above an amount", "batch", "QUERY",
                "SELECT kafka_offset, message_key,\n  JSON_VALUE(message, '$.category') AS category,\n"
                        + "  CAST(JSON_VALUE(message, '$.amount') AS DOUBLE) AS amount\n"
                        + "FROM playground_events\nWHERE CAST(JSON_VALUE(message, '$.amount') AS DOUBLE) >= 1000");
        ex(out, "sql", "computed-of-run", "Read the computed results of this run", "batch", "QUERY",
                "SELECT kafka_offset, target_table, execution_id, producer, row_json\n"
                        + "FROM computed_results\nWHERE run_id = '" + run + "'\nORDER BY kafka_offset");
        ex(out, "sql", "insert-amount-review", "Write computed results: amount-only review (INSERT)", "batch", "INSERT",
                "-- Writes a payment_reviews result for this run's payment requests to computed-results.\n"
                        + "-- Run stage 3 afterwards: the ingestion job validates and writes it to Fluss.\n"
                        + "INSERT INTO computed_results\n"
                        + "SELECT 'computed-result/v1', run_id, 'payment_reviews', 'sql-amount-review', 'flink-sql',\n"
                        + "  JSON_OBJECT(\n"
                        + "    KEY 'run_id' VALUE run_id,\n"
                        + "    KEY 'payment_id' VALUE JSON_VALUE(payload, '$.payment_id'),\n"
                        + "    KEY 'supplier_id' VALUE supplier_id,\n"
                        + "    KEY 'amount_minor' VALUE CAST(JSON_VALUE(payload, '$.amount_minor') AS BIGINT),\n"
                        + "    KEY 'currency' VALUE JSON_VALUE(payload, '$.currency'),\n"
                        + "    KEY 'payment_occurred_at' VALUE DATE_FORMAT(occurred_at, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "    KEY 'outcome' VALUE CASE WHEN CAST(JSON_VALUE(payload, '$.amount_minor') AS BIGINT) > 5000000\n"
                        + "                             THEN 'MANUAL_REVIEW' ELSE 'NO_RULE_MATCH' END,\n"
                        + "    KEY 'check_large_payment' VALUE CASE WHEN CAST(JSON_VALUE(payload, '$.amount_minor') AS BIGINT) > 5000000\n"
                        + "                                         THEN 'TRUE' ELSE 'FALSE' END,\n"
                        + "    KEY 'explanation' VALUE 'Amount-only check written from the SQL Workspace',\n"
                        + "    KEY 'source_payment_event_id' VALUE event_id,\n"
                        + "    KEY 'source_kafka_offset' VALUE kafka_offset,\n"
                        + "    KEY 'rule_version' VALUE 'sql-amount-review/v1'\n"
                        + "  )\n"
                        + "FROM business_events\n"
                        + "WHERE run_id = '" + run + "' AND event_type = 'PAYMENT_REQUESTED'");
        ex(out, "sql", "stream-events", "Streaming: follow new business events (until cancelled)", "streaming", "QUERY",
                "SELECT kafka_offset, kafka_timestamp, run_id, event_type, event_id\n"
                        + "FROM business_events /*+ OPTIONS('scan.startup.mode' = 'latest-offset', 'scan.bounded.mode' = 'unbounded') */");
        ex(out, "sql", "stream-playground-totals", "Streaming: running totals of custom events (changelog)", "streaming", "QUERY",
                "SELECT JSON_VALUE(message, '$.category') AS category, COUNT(*) AS records\n"
                        + "FROM playground_events /*+ OPTIONS('scan.bounded.mode' = 'unbounded') */\n"
                        + "GROUP BY JSON_VALUE(message, '$.category')");
        ex(out, "sql", "show-tables", "List the predefined tables", "batch", "SHOW", "SHOW TABLES");

        ex(out, "query", "supplier-point", "supplier_risk row of this run (primary-key filter)", "batch", "QUERY",
                "SELECT run_id, supplier_id, context_version, signal_band, current_beneficiary_masked,\n"
                        + "  unfamiliar_device_evidence, identity_device_id, computed_by, ingested_by\n"
                        + "FROM " + db + ".supplier_risk\nWHERE run_id = '" + run + "' AND supplier_id = '" + sup + "'");
        ex(out, "query", "review-point", "payment_reviews row of this run (primary-key filter)", "batch", "QUERY",
                "SELECT run_id, payment_id, outcome, check_recent_change, check_unfamiliar_device, check_large_payment,\n"
                        + "  context_version_used, rule_version, computed_by, ingested_by\n"
                        + "FROM " + db + ".payment_reviews\nWHERE run_id = '" + run + "' AND payment_id = '" + pay + "'");
        ex(out, "query", "reviews-by-outcome", "Aggregate: reviews by outcome (all runs)", "batch", "QUERY",
                "SELECT outcome, COUNT(*) AS reviews\nFROM " + db + ".payment_reviews\nGROUP BY outcome");
        ex(out, "query", "receipts-count", "Count: event receipts (all runs)", "batch", "QUERY",
                "SELECT COUNT(*) AS receipts\nFROM " + db + ".event_receipts");
        ex(out, "query", "reviews-limit", "Sample: ten payment reviews", "batch", "QUERY",
                "SELECT run_id, payment_id, outcome, rule_version\nFROM " + db + ".payment_reviews\nLIMIT 10");
        ex(out, "query", "supplier-changelog", "Streaming: supplier_risk changelog (until cancelled)", "streaming", "QUERY",
                "SELECT run_id, supplier_id, context_version, signal_band, ingested_by\nFROM " + db + ".supplier_risk");
        return out;
    }

    private static void ex(ArrayNode out, String ws, String id, String title, String mode, String kind, String sql) {
        ObjectNode e = out.addObject();
        e.put("workspace", ws);
        e.put("id", id);
        e.put("title", title);
        e.put("mode", mode);
        e.put("kind", kind);
        e.put("statement", sql);
    }
}
