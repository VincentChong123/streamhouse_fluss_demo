package demo.paymentrisk.app.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import demo.paymentrisk.app.AppConfig;
import demo.paymentrisk.app.infra.FlinkRest;
import demo.paymentrisk.app.infra.FlussGateway;
import demo.paymentrisk.app.infra.KafkaGateway;
import demo.paymentrisk.common.TableSchemas;
import demo.paymentrisk.common.TableSchemas.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * One-shot, idempotent initialisation. Each wait uses a real API operation with bounded
 * retries (no fixed sleeps, no bare TCP checks):
 *
 * <ol>
 *   <li>Kafka Admin: cluster reachable; create the input, computed-results and playground topics
 *       (1 partition each) if missing.</li>
 *   <li>Fluss Admin: coordinator + TabletServer registered; create database and the three
 *       primary-key tables if missing; verify their schema; prove a native lookup works.</li>
 *   <li>Flink REST: a TaskManager is registered and the cluster has task slots.</li>
 * </ol>
 *
 * The bootstrap never emits events and never submits a job: every stage of the workflow starts
 * only when a user asks for it.
 */
public final class Bootstrapper {
    private static final Logger LOG = LoggerFactory.getLogger(Bootstrapper.class);
    private static final Duration CALL = Duration.ofSeconds(8);
    private static final long RETRY_MS = 2_000L;

    private final AppConfig cfg;
    private final BootstrapStatus status = new BootstrapStatus();

    public Bootstrapper(AppConfig cfg) {
        this.cfg = cfg;
    }

    public int run() {
        BootstrapStatus.Step kafka = status.step("kafka", "Kafka topics " + cfg.inputTopic + ", " + cfg.computedTopic + ", " + cfg.playgroundTopic);
        BootstrapStatus.Step fluss = status.step("fluss", "Fluss database " + cfg.flussDatabase + " and tables");
        BootstrapStatus.Step flink = status.step("flink", "Flink cluster (TaskManager and task slots)");
        status.write(cfg.bootstrapStatusFile);
        BootstrapStatus.Step current = kafka;
        try (KafkaGateway k = new KafkaGateway(cfg.kafkaBootstrap);
             FlussGateway f = new FlussGateway(cfg.flussBootstrap, cfg.flussDatabase)) {
            current = kafka;
            initKafka(k, kafka);
            current = fluss;
            initFluss(f, fluss);
            current = flink;
            initFlink(new FlinkRest(cfg.flinkRestUrl), flink);
            status.state = "DONE";
            status.write(cfg.bootstrapStatusFile);
            LOG.info("Bootstrap complete. Open the demo UI at http://localhost:8090");
            return 0;
        } catch (Exception e) {
            current.state = "FAILED";
            current.detail = e.getMessage();
            if (current.hint == null) {
                current.hint = "Check `docker compose ps` and `docker compose logs " + current.id
                        + "`; then re-run `docker compose up -d` (bootstrap is idempotent).";
            }
            status.state = "FAILED";
            status.write(cfg.bootstrapStatusFile);
            LOG.error("Bootstrap FAILED at step '{}': {}", current.label, e.getMessage());
            LOG.error("Hint: {}", current.hint);
            return 1;
        }
    }

    private interface Attempt<T> {
        T call() throws Exception;
    }

    private <T> T retry(BootstrapStatus.Step step, String what, int maxAttempts, Attempt<T> attempt) throws Exception {
        step.state = "RUNNING";
        Exception last = null;
        for (int i = 1; i <= maxAttempts; i++) {
            step.attempts = i;
            try {
                T result = attempt.call();
                if (result != null && !Boolean.FALSE.equals(result)) {
                    return result;
                }
                step.detail = what + ": not ready yet (attempt " + i + "/" + maxAttempts + ")";
            } catch (Exception e) {
                last = e;
                step.detail = what + ": " + rootMessage(e) + " (attempt " + i + "/" + maxAttempts + ")";
            }
            status.write(cfg.bootstrapStatusFile);
            if (i == 1 || i % 5 == 0) {
                LOG.info("[{}] {}", step.id, step.detail);
            }
            Thread.sleep(RETRY_MS);
        }
        throw new IllegalStateException(what + " not ready after " + maxAttempts + " attempts"
                + (last == null ? "" : ": " + rootMessage(last)), last);
    }

    private void initKafka(KafkaGateway k, BootstrapStatus.Step step) throws Exception {
        step.hint = "Kafka broker not reachable at " + cfg.kafkaBootstrap + ". Check `docker compose logs kafka`.";
        KafkaGateway.ClusterInfo c = retry(step, "Kafka cluster", 90, () -> {
            KafkaGateway.ClusterInfo info = k.describeCluster(CALL);
            return info.brokers > 0 ? info : null;
        });
        List<String> created = new ArrayList<>();
        for (String topic : List.of(cfg.inputTopic, cfg.computedTopic, cfg.playgroundTopic)) {
            if (k.createTopic(topic, 1, (short) 1, CALL)) {
                created.add(topic);
            }
            int partitions = retry(step, "topic " + topic, 30, () -> {
                int p = k.partitions(topic, CALL);
                return p > 0 ? p : null;
            });
            if (partitions != 1) {
                step.hint = "This demo relies on one ordered partition per topic. Delete only this demo's data with "
                        + "`docker compose down --volumes` and start again.";
                throw new IllegalStateException("topic " + topic + " has " + partitions + " partitions; expected 1");
            }
        }
        step.state = "DONE";
        step.detail = "Topics " + cfg.inputTopic + ", " + cfg.computedTopic + ", " + cfg.playgroundTopic + " (1 partition, RF 1)"
                + (created.isEmpty() ? " already existed" : "; created " + created) + " on cluster " + c.clusterId
                + " (" + c.brokers + " broker)";
        status.write(cfg.bootstrapStatusFile);
        LOG.info("[kafka] {}", step.detail);
    }

    private void initFluss(FlussGateway f, BootstrapStatus.Step step) throws Exception {
        step.hint = "Fluss not ready at " + cfg.flussBootstrap
                + ". Check `docker compose logs fluss-coordinator fluss-tablet zookeeper`.";
        FlussGateway.Nodes nodes = retry(step, "Fluss coordinator + TabletServer", 90, () -> {
            FlussGateway.Nodes n = f.serverNodes(CALL);
            return n.coordinators > 0 && n.tabletServers > 0 ? n : null;
        });
        retry(step, "create database " + cfg.flussDatabase, 30, () -> {
            f.createDatabase(CALL);
            return Boolean.TRUE;
        });
        List<String> created = new ArrayList<>();
        for (TableSpec spec : TableSchemas.all()) {
            boolean c = retry(step, "create table " + spec.name, 30, () -> f.createTable(spec, CALL) ? "created" : "exists")
                    .equals("created");
            if (c) {
                created.add(spec.name);
            }
            List<String> diffs = f.schemaDifferences(spec, CALL);
            if (!diffs.isEmpty()) {
                step.hint = "An older table layout exists in this demo's Fluss volumes. Run "
                        + "`docker compose down --volumes` (deletes only this demo's data), then start again.";
                throw new IllegalStateException(spec.name + ": " + String.join("; ", diffs));
            }
        }
        // Prove the read path end to end: a native primary-key lookup on every table.
        for (TableSpec spec : TableSchemas.all()) {
            List<String> probeKey = new ArrayList<>();
            for (int i = 0; i < spec.primaryKey.size(); i++) {
                probeKey.add("__bootstrap_probe__");
            }
            retry(step, "lookup probe on " + spec.name, 60, () -> {
                FlussGateway.Observation o = f.lookup(spec, probeKey, CALL);
                if (!o.ok) {
                    throw new IllegalStateException(o.error);
                }
                return Boolean.TRUE;
            });
        }
        step.state = "DONE";
        step.detail = "Servers " + nodes.endpoints + "; tables " + TableSchemas.all().stream().map(s -> s.name).toList()
                + (created.isEmpty() ? " already existed" : " (created " + created + ")") + "; lookup probes OK";
        status.write(cfg.bootstrapStatusFile);
        LOG.info("[fluss] {}", step.detail);
    }

    private void initFlink(FlinkRest flink, BootstrapStatus.Step step) throws Exception {
        step.hint = "Flink not ready at " + cfg.flinkRestUrl
                + ". Check `docker compose logs flink-jobmanager flink-taskmanager`.";
        retry(step, "Flink TaskManager registration", 90, () -> {
            JsonNode o = flink.overview();
            return o.path("taskmanagers").asInt() > 0 ? o : null;
        });

        JsonNode o = flink.overview();
        step.state = "DONE";
        step.detail = "Flink " + o.path("flink-version").asText() + ": " + o.path("taskmanagers").asInt() + " TaskManager, "
                + o.path("slots-total").asInt() + " task slots. No job was submitted: jobs run only when requested.";
        status.write(cfg.bootstrapStatusFile);
        LOG.info("[flink] {}", step.detail);
    }

    static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
