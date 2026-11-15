package demo.paymentrisk.app.health;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.app.AppConfig;
import demo.paymentrisk.app.bootstrap.BootstrapStatus;
import demo.paymentrisk.app.infra.FlinkRest;
import demo.paymentrisk.app.infra.FlussGateway;
import demo.paymentrisk.app.infra.KafkaGateway;
import demo.paymentrisk.app.infra.PythonRunnerClient;
import demo.paymentrisk.app.infra.SqlGatewayClient;
import demo.paymentrisk.common.Json;
import demo.paymentrisk.common.TableSchemas;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Periodic health checks using real operations: Kafka Admin, Fluss Admin plus a native lookup
 * probe, the Flink REST API, the SQL Gateway REST API and the Python runner. Health is shown in
 * the header; it never advances a stage.
 */
public final class HealthMonitor {
    private static final Duration CALL = Duration.ofSeconds(3);

    public enum Status { UP, PENDING, DOWN }

    public static final class Component {
        public final String id;
        public final String label;
        public final boolean required;
        public volatile Status status = Status.PENDING;
        public volatile String detail = "checking…";
        public volatile String checkedAt;
        public volatile String hint;

        Component(String id, String label, boolean required) {
            this.id = id;
            this.label = label;
            this.required = required;
        }
    }

    private final AppConfig cfg;
    private final KafkaGateway kafka;
    private final FlussGateway fluss;
    private final FlinkRest flink;
    private final SqlGatewayClient sqlGateway;
    private final PythonRunnerClient python;
    private final Runnable onChange;
    private final Map<String, Component> components = new LinkedHashMap<>();
    private volatile boolean everReady;
    private volatile BootstrapStatus bootstrap;
    private volatile int slotsTotal;
    private volatile int slotsAvailable;

    public HealthMonitor(AppConfig cfg, KafkaGateway kafka, FlussGateway fluss, FlinkRest flink,
                         SqlGatewayClient sqlGateway, PythonRunnerClient python, Runnable onChange) {
        this.cfg = cfg;
        this.kafka = kafka;
        this.fluss = fluss;
        this.flink = flink;
        this.sqlGateway = sqlGateway;
        this.python = python;
        this.onChange = onChange;
        add("kafka", "Kafka", true);
        add("flink", "Flink", true);
        add("fluss", "Fluss", true);
        add("sql_gateway", "SQL Gateway", false);
        add("python_runner", "Python runner", false);
    }

    private void add(String id, String label, boolean required) {
        components.put(id, new Component(id, label, required));
    }

    public void start() {
        Thread t = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                checkOnce();
                try {
                    Thread.sleep(isReady() ? 3_000 : 1_500);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "health-monitor");
        t.setDaemon(true);
        t.start();
    }

    /** True when Kafka, Flink and Fluss are UP (the services the four stages need). */
    public boolean isReady() {
        for (Component c : components.values()) {
            if (c.required && c.status != Status.UP) {
                return false;
            }
        }
        return true;
    }

    public void checkOnce() {
        checkKafka(components.get("kafka"));
        checkFlink(components.get("flink"));
        checkFluss(components.get("fluss"));
        checkSqlGateway(components.get("sql_gateway"));
        checkPython(components.get("python_runner"));
        bootstrap = BootstrapStatus.read(cfg.bootstrapStatusFile);
        if (isReady()) {
            everReady = true;
        }
        onChange.run();
    }

    private void set(Component c, Status s, String detail, String hint) {
        c.status = s;
        c.detail = detail;
        c.hint = hint;
        c.checkedAt = Instant.now().toString();
    }

    private void checkKafka(Component c) {
        try {
            KafkaGateway.ClusterInfo info = kafka.describeCluster(CALL);
            for (String topic : List.of(cfg.inputTopic, cfg.computedTopic, cfg.playgroundTopic)) {
                if (kafka.partitions(topic, CALL) < 0) {
                    set(c, Status.PENDING, info.brokers + " broker reachable; waiting for the bootstrap to create topic " + topic,
                            "See `docker compose logs bootstrap`.");
                    return;
                }
            }
            set(c, Status.UP, info.brokers + " broker (KRaft); topics " + cfg.inputTopic + ", " + cfg.computedTopic + ", "
                    + cfg.playgroundTopic, null);
        } catch (Exception e) {
            set(c, Status.DOWN, "Kafka Admin call failed: " + root(e), "Check `docker compose ps kafka` and `docker compose logs kafka`.");
        }
    }

    private void checkFlink(Component c) {
        try {
            JsonNode o = flink.overview();
            int tms = o.path("taskmanagers").asInt();
            slotsTotal = o.path("slots-total").asInt();
            slotsAvailable = o.path("slots-available").asInt();
            String ver = o.path("flink-version").asText();
            if (tms == 0) {
                set(c, Status.DOWN, "Flink " + ver + ": no TaskManager registered; jobs cannot run",
                        "Start it with `docker compose start flink-taskmanager`.");
            } else {
                set(c, Status.UP, "Flink " + ver + ": " + tms + " TaskManager, " + slotsAvailable + " of " + slotsTotal
                        + " task slots free, " + o.path("jobs-running").asInt() + " job(s) running", null);
            }
        } catch (Exception e) {
            set(c, Status.DOWN, "Flink REST call failed: " + root(e), "Check `docker compose logs flink-jobmanager`.");
        }
    }

    private void checkFluss(Component c) {
        try {
            FlussGateway.Nodes n = fluss.serverNodes(CALL);
            if (n.coordinators == 0 || n.tabletServers == 0) {
                set(c, Status.DOWN, "Fluss servers registered: " + n.endpoints + " (needs a coordinator and a TabletServer)",
                        "Check `docker compose ps fluss-tablet fluss-coordinator`.");
                return;
            }
            for (TableSchemas.TableSpec t : TableSchemas.all()) {
                if (!fluss.tableExists(t, CALL)) {
                    set(c, Status.PENDING, "Waiting for the bootstrap to create " + cfg.flussDatabase + "." + t.name,
                            "See `docker compose logs bootstrap`.");
                    return;
                }
            }
            FlussGateway.Observation probe = fluss.lookup(TableSchemas.SUPPLIER_RISK, List.of("__health__", "__health__"), CALL);
            if (!probe.ok) {
                set(c, Status.DOWN, "Native lookup probe failed: " + probe.error, "Check `docker compose logs fluss-tablet`.");
                return;
            }
            set(c, Status.UP, n.coordinators + " coordinator, " + n.tabletServers + " TabletServer; 3 primary-key tables; lookup probe "
                    + String.format("%.1f", probe.durationMs) + " ms", null);
        } catch (Exception e) {
            set(c, Status.DOWN, "Fluss Admin call failed: " + root(e), "Check `docker compose logs fluss-coordinator`.");
        }
    }

    private void checkSqlGateway(Component c) {
        try {
            JsonNode i = sqlGateway.info();
            set(c, Status.UP, i.path("productName").asText("Flink") + " " + i.path("version").asText() + " SQL Gateway (REST v2)", null);
        } catch (Exception e) {
            set(c, Status.DOWN, "SQL Gateway not reachable: " + root(e), "Check `docker compose logs flink-sql-gateway`.");
        }
    }

    private void checkPython(Component c) {
        if (!cfg.pythonEnabled) {
            set(c, Status.DOWN, "Disabled (PLAYGROUND_PYTHON_ENABLED=false)", null);
            return;
        }
        try {
            JsonNode h = python.health();
            set(c, Status.UP, "Python " + h.path("python").asText() + ", pyfluss " + h.path("pyfluss").asText(), null);
        } catch (Exception e) {
            set(c, Status.DOWN, "Python runner not reachable: " + root(e), "Check `docker compose logs python-runner`.");
        }
    }

    public ObjectNode toJson() {
        ObjectNode h = Json.mapper().createObjectNode();
        boolean ready = isReady();
        h.put("overall", ready ? "READY" : everReady ? "DEGRADED" : "STARTING");
        ArrayNode arr = h.putArray("components");
        for (Component c : components.values()) {
            ObjectNode o = arr.addObject();
            o.put("id", c.id);
            o.put("label", c.label);
            o.put("required", c.required);
            o.put("status", c.status.name());
            o.put("detail", c.detail);
            o.put("hint", c.hint);
            o.put("checked_at", c.checkedAt);
        }
        h.put("slots_total", slotsTotal);
        h.put("slots_available", slotsAvailable);
        h.put("flink_ui_url", cfg.flinkUiPublicUrl);
        h.set("bootstrap", bootstrap == null ? null : Json.mapper().valueToTree(bootstrap));
        return h;
    }

    private static String root(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
