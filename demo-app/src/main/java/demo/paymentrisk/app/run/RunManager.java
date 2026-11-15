package demo.paymentrisk.app.run;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.app.AppConfig;
import demo.paymentrisk.app.infra.FlinkRest;
import demo.paymentrisk.app.infra.FlussGateway;
import demo.paymentrisk.app.infra.KafkaGateway;
import demo.paymentrisk.app.scenario.EventFactory;
import demo.paymentrisk.app.scenario.Scenario;
import demo.paymentrisk.common.ComputedResults;
import demo.paymentrisk.common.Json;
import demo.paymentrisk.common.TableSchemas;
import demo.paymentrisk.common.TableSchemas.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs the four stages, one operation at a time, each only when a user asks for it:
 *
 * <ol>
 *   <li>Emit the scenario's business events to Kafka and record each acknowledgment.</li>
 *   <li>Submit the bounded processing job over this run's captured input offsets; read the
 *       computed results it wrote to the computed-results topic.</li>
 *   <li>Submit the bounded ingestion job over this run's computed results; confirm every row
 *       with a native Fluss lookup.</li>
 *   <li>Query Fluss with native primary-key lookups.</li>
 * </ol>
 *
 * Nothing runs on startup or page load. Every status shown comes from Kafka, the Flink REST API
 * or Fluss; a timeout or failure is reported as such.
 */
public final class RunManager {
    private static final Logger LOG = LoggerFactory.getLogger(RunManager.class);
    private static final Duration CALL = Duration.ofSeconds(8);
    private static final DateTimeFormatter RUN_ID = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final Set<String> TERMINAL = Set.of("FINISHED", "FAILED", "CANCELED");

    /** 409: another operation is running, or the stage's precondition is not met. */
    public static final class ConflictException extends Exception {
        public ConflictException(String message) {
            super(message);
        }
    }

    private final AppConfig cfg;
    private final KafkaGateway kafka;
    private final FlussGateway fluss;
    private final FlinkRest flink;
    private final Scenario scenario;
    private final ExecutionHistory history;
    private final Runnable onChange;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "stage-worker");
        t.setDaemon(true);
        return t;
    });
    /** Runs independent Fluss lookups concurrently (read-back rows, the two stage-4 lookups). */
    private final ExecutorService lookups = Executors.newFixedThreadPool(8, r -> {
        Thread t = new Thread(r, "fluss-lookup");
        t.setDaemon(true);
        return t;
    });
    private final AtomicReference<String> busy = new AtomicReference<>();
    private final AtomicInteger runsCreated = new AtomicInteger();
    private volatile Run current;
    private volatile String jarId;

    public RunManager(AppConfig cfg, KafkaGateway kafka, FlussGateway fluss, FlinkRest flink, Scenario scenario,
                      ExecutionHistory history, Runnable onChange) {
        this.cfg = cfg;
        this.kafka = kafka;
        this.fluss = fluss;
        this.flink = flink;
        this.scenario = scenario;
        this.history = history;
        this.onChange = onChange;
    }

    public Run current() {
        return current;
    }

    public String busyOperation() {
        return busy.get();
    }

    public int runsCreated() {
        return runsCreated.get();
    }

    // ------------------------------------------------------------------ runs

    /** Creates a fresh run (new run ID). Topics and tables are kept; earlier runs' data stays where it is. */
    public synchronized Run newRun() throws ConflictException {
        String op = busy.get();
        if (op != null) {
            throw new ConflictException("Wait for the running operation (" + op + ") to finish before starting a new demo.");
        }
        String id = "run-" + LocalDateTime.now(ZoneOffset.UTC).format(RUN_ID) + "-" + UUID.randomUUID().toString().substring(0, 4);
        Run r = new Run(id, scenario, cfg.inputTopic);
        try {
            captureOffsets(r);
        } catch (Exception e) {
            LOG.warn("Could not capture start offsets for {} yet ({}); will capture before the first operation", id, root(e));
        }
        current = r;
        runsCreated.incrementAndGet();
        LOG.info("New run {} (input from offset {}, computed results from offset {})", id, r.inputStartOffset, r.computedStartOffset);
        onChange.run();
        return r;
    }

    /** Records the log-end offsets of both topics: every record of this run comes after them. */
    public void captureOffsets(Run r) throws Exception {
        synchronized (r) {
            if (r.inputStartOffset == null) {
                r.inputStartOffset = kafka.endOffset(cfg.inputTopic, 0, CALL);
            }
            if (r.computedStartOffset == null) {
                r.computedStartOffset = kafka.endOffset(cfg.computedTopic, 0, CALL);
            }
        }
    }

    /** Sets canRun / blockedReason on every stage (called when a snapshot is built). */
    public void computeGates(Run r) {
        String op = busy.get();
        String busyReason = op == null ? null : "Waiting for " + op + " to finish.";
        gate(r.stage1, busyReason != null ? busyReason
                : r.stage1.status == Run.Status.SUCCEEDED ? "This run's scenario events are already in Kafka. Start a new demo to emit them again."
                : null);
        gate(r.stage2, busyReason != null ? busyReason
                : r.stage1.status != Run.Status.SUCCEEDED ? "Requires stage 1: emit the scenario events first."
                : null);
        gate(r.stage3, busyReason != null ? busyReason
                : r.stage2.status != Run.Status.SUCCEEDED ? "Requires stage 2: run Flink processing successfully first."
                : null);
        gate(r.stage4, busyReason != null ? busyReason
                : r.stage3.status != Run.Status.SUCCEEDED ? "Requires stage 3: Fluss must confirm the ingested rows first."
                : null);
    }

    private static void gate(Run.Stage s, String reason) {
        s.canRun = reason == null;
        s.blockedReason = reason;
    }

    private void submit(String op, Run.Stage stage, Runnable work) throws ConflictException {
        Run r = current;
        if (r == null) {
            throw new ConflictException("No run exists yet; start a new demo.");
        }
        synchronized (this) {
            computeGates(r);
            if (!stage.canRun) {
                throw new ConflictException(stage.blockedReason);
            }
            if (!busy.compareAndSet(null, op)) {
                throw new ConflictException("Another operation is running (" + busy.get() + ").");
            }
            stage.begin();
            stage.phase = "Queued";
            stage.step("Queued");
        }
        onChange.run();
        worker.submit(() -> {
            try {
                work.run();
            } catch (Exception e) {
                LOG.error("{} failed unexpectedly", op, e);
                if (stage.status == Run.Status.RUNNING) {
                    fail(stage, "Unexpected error: " + root(e), null);
                }
            } finally {
                busy.set(null);
                onChange.run();
            }
        });
    }

    private void fail(Run.Stage s, String error, String detail) {
        s.error = error;
        s.errorDetail = detail;
        s.end(Run.Status.FAILED);
        onChange.run();
    }

    /** Shows what the stage is doing and starts timing it as a step (see {@link Run.Stage#steps}). */
    private void phase(Run.Stage s, String text) {
        s.phase = text;
        s.step(text);
        onChange.run();
    }

    // ------------------------------------------------------------------ stage 1: Kafka

    public void emitScenario() throws ConflictException {
        Run r = current;
        submit("stage 1 (emit scenario events)", r == null ? null : r.stage1, () -> emit(r));
    }

    private void emit(Run r) {
        Run.Stage1 s = r.stage1;
        ExecutionHistory.Entry h = history.add("emit-" + shortId(r) + "-" + s.executions, "SCENARIO_EVENTS",
                "Emit " + s.events.size() + " scenario events to " + cfg.inputTopic, r.runId);
        try {
            phase(s, "Capturing the topic's start offset");
            captureOffsets(r);
        } catch (Exception e) {
            fail(s, "Kafka is not reachable, so no event was sent: " + root(e), null);
            h.finish("FAILED", s.error);
            return;
        }
        int sent = 0;
        for (int i = 0; i < s.events.size(); i++) {
            Run.EventView ev = s.events.get(i);
            if ("ACKNOWLEDGED".equals(ev.status)) {
                continue; // a retry after a failure resends only what Kafka did not acknowledge
            }
            if (sent > 0 && cfg.emitPaceMs > 0) {
                s.step("Display pause between events");
                sleep(cfg.emitPaceMs); // rendering pace only; each event below still waits for its own ack
            }
            Scenario.Step step = scenario.steps.get(i);
            ObjectNode record = EventFactory.build(r.runId, r.correlationId, r.supplierId, step, ev.eventId,
                    Instant.parse(ev.occurredAt), Instant.now());
            ev.record = record;
            ev.status = "SENDING";
            ev.error = null;
            phase(s, "Sending " + ev.eventType + " (" + (i + 1) + "/" + s.events.size() + ")");
            try {
                KafkaGateway.Ack ack = kafka.send(cfg.inputTopic, ev.key, Json.write(record), Duration.ofMillis(cfg.sendTimeoutMs));
                ev.partition = ack.partition;
                ev.offset = ack.offset;
                ev.kafkaTimestamp = Instant.ofEpochMilli(ack.kafkaTimestampMs).toString();
                ev.ackedAt = Instant.ofEpochMilli(ack.ackedAtMs).toString();
                ev.sendMs = ack.ackedAtMs - ack.sendStartedMs;
                ev.status = "ACKNOWLEDGED";
                sent++;
                onChange.run();
            } catch (Exception e) {
                ev.status = "FAILED";
                ev.error = root(e);
                fail(s, "Kafka did not acknowledge " + ev.eventType + " " + ev.eventId + ": " + ev.error
                        + ". Events acknowledged so far are kept; retry sends only the rest.", null);
                h.finish("FAILED", s.error);
                return;
            }
        }
        s.end(Run.Status.SUCCEEDED);
        Run.EventView first = s.events.get(0);
        Run.EventView last = s.events.get(s.events.size() - 1);
        h.finish("SUCCEEDED", s.events.size() + " records acknowledged (acks=all) at " + cfg.inputTopic + "-0 offsets "
                + first.offset + "…" + last.offset);
        onChange.run();
    }

    // ------------------------------------------------------------------ stage 2: Flink processing

    public void runProcessing() throws ConflictException {
        Run r = current;
        submit("stage 2 (Flink processing)", r == null ? null : r.stage2, () -> process(r));
    }

    private void process(Run r) {
        Run.Stage2 s = r.stage2;
        r.stage3.reset();
        r.stage3.execution = null;
        r.stage4.reset();
        r.stage4.result = null;
        Run.ProcessingExecution x = new Run.ProcessingExecution();
        x.executionId = "proc-" + shortId(r) + "-" + s.executions;
        x.inputTopic = cfg.inputTopic;
        x.outputTopic = cfg.computedTopic;
        s.execution = x;
        ExecutionHistory.Entry h = history.add(x.executionId, "PROCESSING_JOB", "Flink processing job (bounded)", r.runId);
        try {
            phase(s, "Checking the Flink cluster");
            String unavailable = flinkUnavailable();
            if (unavailable != null) {
                fail(s, unavailable, null);
                h.finish("FAILED", s.error);
                return;
            }
            phase(s, "Capturing this run's input offset range");
            captureOffsets(r);
            x.inputStart = r.inputStartOffset;
            x.inputEnd = kafka.endOffset(cfg.inputTopic, 0, CALL);
            List<KafkaGateway.Rec> input = kafka.read(cfg.inputTopic, 0, x.inputStart, x.inputEnd, 20_000, Duration.ofSeconds(20));
            int mine = 0;
            List<String> ids = new ArrayList<>();
            for (KafkaGateway.Rec rec : input) {
                JsonNode n = parseOrNull(rec.value);
                if (n != null && r.runId.equals(n.path("run_id").asText(null))) {
                    mine++;
                    ids.add(n.path("event_id").asText());
                }
            }
            x.inputRecordsThisRun = mine;
            x.inputRecordsOtherRuns = input.size() - mine;
            x.inputEventIds = ids;
            x.outputStart = kafka.endOffset(cfg.computedTopic, 0, CALL);
            onChange.run();

            List<String> args = List.of("--job", "processing", "--job-name", "payment-risk processing " + x.executionId,
                    "--run-id", r.runId, "--execution-id", x.executionId,
                    "--kafka-bootstrap", cfg.kafkaBootstrap, "--source-topic", cfg.inputTopic, "--source-partition", "0",
                    "--start-offset", String.valueOf(x.inputStart), "--end-offset", String.valueOf(x.inputEnd),
                    "--output-topic", cfg.computedTopic, "--threshold-minor", String.valueOf(cfg.ruleThresholdMinor));
            if (!runJob(s, x, args, h)) {
                return;
            }

            phase(s, "Reading the computed results from " + cfg.computedTopic);
            x.outputEnd = kafka.endOffset(cfg.computedTopic, 0, CALL);
            List<Run.ResultView> results = new ArrayList<>();
            for (KafkaGateway.Rec rec : kafka.read(cfg.computedTopic, 0, x.outputStart, x.outputEnd, 20_000, Duration.ofSeconds(20))) {
                ComputedResults.Parsed p = ComputedResults.parse(rec.value);
                if (x.executionId.equals(p.executionId) && r.runId.equals(p.runId)) {
                    results.add(view(rec, p));
                }
            }
            x.results = results;
            if (results.isEmpty()) {
                fail(s, "The job finished but wrote no computed results for this run (input range had " + mine
                        + " record(s) of this run).", null);
                h.finish("FAILED", s.error);
                return;
            }
            s.end(Run.Status.SUCCEEDED);
            h.finish("SUCCEEDED", "Read " + mine + " event(s) from " + cfg.inputTopic + " [" + x.inputStart + ", " + x.inputEnd
                    + "); wrote " + results.size() + " computed result(s) to " + cfg.computedTopic + " [" + x.outputStart
                    + ", " + x.outputEnd + ")");
            onChange.run();
        } catch (Exception e) {
            fail(s, root(e), null);
            h.finish("FAILED", s.error);
        }
    }

    // ------------------------------------------------------------------ stage 3: Fluss ingestion

    public void runIngestion() throws ConflictException {
        Run r = current;
        submit("stage 3 (Fluss ingestion)", r == null ? null : r.stage3, () -> ingest(r));
    }

    private void ingest(Run r) {
        Run.Stage3 s = r.stage3;
        r.stage4.reset();
        r.stage4.result = null;
        Run.IngestionExecution x = new Run.IngestionExecution();
        x.executionId = "ingest-" + shortId(r) + "-" + s.executions;
        x.sourceTopic = cfg.computedTopic;
        s.execution = x;
        ExecutionHistory.Entry h = history.add(x.executionId, "INGESTION_JOB", "Fluss ingestion job (bounded)", r.runId);
        try {
            phase(s, "Checking Fluss and Flink");
            String flussDown = flussUnavailable();
            String flinkDown = flussDown == null ? flinkUnavailable() : null;
            if (flussDown != null || flinkDown != null) {
                fail(s, flussDown != null ? flussDown : flinkDown, null);
                h.finish("FAILED", s.error);
                return;
            }
            phase(s, "Capturing and validating this run's computed results");
            captureOffsets(r);
            x.sourceStart = r.computedStartOffset;
            x.sourceEnd = kafka.endOffset(cfg.computedTopic, 0, CALL);
            Map<String, Run.ReadBackRow> latest = new LinkedHashMap<>();
            Map<String, Integer> byProducer = new LinkedHashMap<>();
            List<Run.ResultView> versions = new ArrayList<>();
            int mine = 0;
            int other = 0;
            for (KafkaGateway.Rec rec : kafka.read(cfg.computedTopic, 0, x.sourceStart, x.sourceEnd, 20_000, Duration.ofSeconds(20))) {
                ComputedResults.Parsed p = ComputedResults.parse(rec.value);
                if (p.runId != null && !r.runId.equals(p.runId)) {
                    other++;
                    continue;
                }
                mine++;
                if (!p.ok()) {
                    Run.Rejected rj = new Run.Rejected();
                    rj.offset = rec.offset;
                    rj.executionId = p.executionId;
                    rj.targetTable = p.targetTable;
                    rj.errors = p.errors;
                    x.rejected.add(rj);
                    continue;
                }
                byProducer.merge(p.executionId + " (" + Objects.toString(p.producer, "unknown producer") + ")", 1, Integer::sum);
                if (p.spec == TableSchemas.SUPPLIER_RISK) {
                    versions.add(view(rec, p));
                }
                Run.ReadBackRow rb = new Run.ReadBackRow();
                rb.table = p.spec.name;
                rb.key = new LinkedHashMap<>();
                List<String> kv = p.keyValues();
                for (int i = 0; i < kv.size(); i++) {
                    rb.key.put(p.spec.primaryKey.get(i), kv.get(i));
                }
                rb.computedBy = p.executionId;
                rb.sourceOffset = rec.offset;
                rb.expected = displayValues(p);
                latest.put(p.spec.name + "|" + String.join("|", kv), rb); // a later record for the same key wins
            }
            x.recordsThisRun = mine;
            x.recordsOtherRuns = other;
            x.recordsValid = mine - x.rejected.size();
            x.byProducer = byProducer;
            x.supplierVersions = versions;
            for (TableSpec spec : TableSchemas.all()) {
                Run.Destination d = new Run.Destination();
                d.table = cfg.flussDatabase + "." + spec.name;
                d.primaryKey = spec.primaryKey;
                d.rowsExpected = (int) latest.values().stream().filter(v -> v.table.equals(spec.name)).count();
                x.destinations.add(d);
            }
            onChange.run();
            if (x.recordsValid == 0) {
                fail(s, "No valid computed results for this run in " + cfg.computedTopic + " [" + x.sourceStart + ", " + x.sourceEnd
                        + ")" + (x.rejected.isEmpty() ? ". Run Flink processing first." : "; " + x.rejected.size()
                        + " record(s) do not match the computed-result/v1 contract (see below)."), null);
                h.finish("FAILED", s.error);
                return;
            }

            List<String> args = List.of("--job", "ingestion", "--job-name", "payment-risk ingestion " + x.executionId,
                    "--run-id", r.runId, "--execution-id", x.executionId,
                    "--kafka-bootstrap", cfg.kafkaBootstrap, "--source-topic", cfg.computedTopic, "--source-partition", "0",
                    "--start-offset", String.valueOf(x.sourceStart), "--end-offset", String.valueOf(x.sourceEnd),
                    "--fluss-bootstrap", cfg.flussBootstrap, "--database", cfg.flussDatabase);
            if (!runJob(s, x, args, h)) {
                return;
            }

            phase(s, "Confirming every row with a native Fluss lookup");
            x.readBack.method = FlussGateway.READ_METHOD;
            x.readBack.rows.addAll(latest.values());
            boolean ok = false;
            for (int attempt = 1; attempt <= 3 && !ok; attempt++) {
                if (attempt > 1) {
                    sleep(300);
                }
                ok = verifyRows(x);
                onChange.run();
            }
            if (!ok) {
                int missing = x.readBack.checked - x.readBack.matched;
                fail(s, "The ingestion job finished, but " + missing + " of " + x.readBack.checked
                        + " row(s) could not be confirmed with a Fluss lookup (see the read-back table).", null);
                h.finish("FAILED", s.error);
                return;
            }
            if (!x.rejected.isEmpty()) {
                s.warning = x.rejected.size() + " record(s) of this run did not match the computed-result/v1 contract and were not written.";
            }
            s.end(Run.Status.SUCCEEDED);
            h.finish("SUCCEEDED", "Wrote and confirmed " + x.readBack.matched + " Fluss row(s) from " + x.recordsValid
                    + " computed result(s)" + (x.rejected.isEmpty() ? "" : "; " + x.rejected.size() + " rejected"));
            onChange.run();
        } catch (Exception e) {
            fail(s, root(e), null);
            h.finish("FAILED", s.error);
        }
    }

    private boolean verifyRows(Run.IngestionExecution x) {
        // Issue every row's lookup at once; each Observation still records its own timing.
        List<CompletableFuture<FlussGateway.Observation>> pending = new ArrayList<>();
        for (Run.ReadBackRow rb : x.readBack.rows) {
            TableSpec spec = TableSchemas.byName(rb.table);
            List<String> key = new ArrayList<>(rb.key.values());
            pending.add(CompletableFuture.supplyAsync(() -> fluss.lookup(spec, key, Duration.ofSeconds(3)), lookups));
        }
        int matched = 0;
        for (int i = 0; i < x.readBack.rows.size(); i++) {
            Run.ReadBackRow rb = x.readBack.rows.get(i);
            FlussGateway.Observation o = pending.get(i).join();
            rb.observation = o;
            List<String> mismatches = new ArrayList<>();
            if (!o.ok) {
                mismatches.add("lookup failed: " + o.error);
            } else if (!o.found) {
                mismatches.add("no row with this primary key");
            } else {
                for (Map.Entry<String, Object> e : rb.expected.entrySet()) {
                    String want = e.getValue() == null ? null : String.valueOf(e.getValue());
                    Object got = o.row.get(e.getKey());
                    String gotS = got == null ? null : String.valueOf(got);
                    if (!Objects.equals(want, gotS)) {
                        mismatches.add(e.getKey() + ": expected " + want + ", Fluss has " + gotS);
                    }
                }
                if (!rb.computedBy.equals(o.row.get("computed_by"))) {
                    mismatches.add("computed_by: expected " + rb.computedBy + ", Fluss has " + o.row.get("computed_by"));
                }
                if (!x.executionId.equals(o.row.get("ingested_by"))) {
                    mismatches.add("ingested_by: expected " + x.executionId + ", Fluss has " + o.row.get("ingested_by"));
                }
            }
            rb.mismatches = mismatches;
            rb.matches = mismatches.isEmpty();
            if (rb.matches) {
                matched++;
            }
        }
        x.readBack.checked = x.readBack.rows.size();
        x.readBack.matched = matched;
        x.readBack.verified = matched == x.readBack.checked && matched > 0;
        return x.readBack.verified;
    }

    // ------------------------------------------------------------------ stage 4: query Fluss

    public void query() throws ConflictException {
        Run r = current;
        submit("stage 4 (query Fluss)", r == null ? null : r.stage4, () -> lookup(r));
    }

    private void lookup(Run r) {
        Run.Stage4 s = r.stage4;
        ExecutionHistory.Entry h = history.add("query-" + shortId(r) + "-" + s.executions, "FLUSS_LOOKUP",
                "Native Fluss lookups: supplier_risk + payment_reviews", r.runId);
        phase(s, "Looking up supplier_risk and payment_reviews");
        Run.QueryResult q = new Run.QueryResult();
        q.queriedAt = Instant.now().toString();
        q.mechanism = FlussGateway.READ_METHOD;
        CompletableFuture<FlussGateway.Observation> supplier = CompletableFuture.supplyAsync(
                () -> fluss.lookup(TableSchemas.SUPPLIER_RISK, List.of(r.runId, r.supplierId), Duration.ofSeconds(3)), lookups);
        CompletableFuture<FlussGateway.Observation> review = CompletableFuture.supplyAsync(
                () -> fluss.lookup(TableSchemas.PAYMENT_REVIEWS, List.of(r.runId, r.paymentId), Duration.ofSeconds(3)), lookups);
        q.supplier = supplier.join();
        q.review = review.join();
        s.result = q;
        if (!q.supplier.ok || !q.review.ok) {
            fail(s, "Fluss lookup failed: " + (q.supplier.ok ? q.review.error : q.supplier.error), null);
            h.finish("FAILED", s.error);
            return;
        }
        if (!q.supplier.found || !q.review.found) {
            fail(s, "Fluss returned no row for " + (q.supplier.found ? "payment_reviews " + r.paymentId : "supplier_risk " + r.supplierId)
                    + " in this run.", null);
            h.finish("FAILED", s.error);
            return;
        }
        s.end(Run.Status.SUCCEEDED);
        h.finish("SUCCEEDED", "supplier_risk v" + q.supplier.row.get("context_version") + " (" + q.supplier.row.get("signal_band")
                + "), payment_reviews " + q.review.row.get("outcome") + String.format(" in %.1f + %.1f ms",
                q.supplier.durationMs, q.review.durationMs));
        onChange.run();
    }

    // ------------------------------------------------------------------ Flink job helpers

    /** Submits the bounded job and waits for a terminal state. Returns false (stage failed) otherwise. */
    private boolean runJob(Run.Stage s, Run.JobExecution x, List<String> args, ExecutionHistory.Entry h) throws Exception {
        phase(s, "Submitting the job to Flink");
        x.programArgs = args;
        x.jobName = args.get(args.indexOf("--job-name") + 1);
        String jobId;
        try {
            jobId = flink.runJar(ensureJar(), cfg.flinkEntryClass, args, 1, null);
        } catch (Exception e) {
            fail(s, "Flink rejected the job submission: " + root(e), String.valueOf(e.getMessage()));
            h.finish("FAILED", s.error);
            return false;
        }
        x.jobId = jobId;
        x.submittedAt = Instant.now().toString();
        x.jobUrl = cfg.flinkUiPublicUrl + "/#/job/running/" + jobId + "/overview";
        h.jobId = jobId;
        h.jobUrl = x.jobUrl;
        phase(s, "Job submitted; waiting for Flink to report a final state");
        long deadline = System.currentTimeMillis() + cfg.stageJobTimeoutMs;
        String state = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                JsonNode j = flink.job(jobId);
                state = j.path("state").asText();
                if (TERMINAL.contains(state)) {
                    x.jobState = state;
                    x.jobDurationMs = j.path("duration").asLong();
                    long end = j.path("end-time").asLong(-1);
                    x.jobFinishedAt = end > 0 ? Instant.ofEpochMilli(end).toString() : null;
                    break;
                }
                if (!state.equals(x.jobState)) {
                    x.jobState = state;
                    phase(s, "Flink job " + state);
                }
            } catch (Exception e) {
                x.jobState = "UNKNOWN";
                phase(s, "Flink REST not answering: " + root(e));
            }
            sleep(cfg.jobPollMs);
        }
        x.jobUrl = cfg.flinkUiPublicUrl + "/#/job/" + (TERMINAL.contains(String.valueOf(state)) ? "completed" : "running")
                + "/" + jobId + "/overview";
        h.jobUrl = x.jobUrl;
        phase(s, "Flink job " + Objects.toString(state, "state unknown") + "; reading its accumulators");
        try {
            x.accumulators = flink.accumulators(jobId);
        } catch (Exception ignored) {
            // shown as absent
        }
        if (!"FINISHED".equals(state)) {
            String why;
            if (state == null || !TERMINAL.contains(state)) {
                try {
                    flink.cancel(jobId);
                } catch (Exception ignored) {
                    // reported below
                }
                why = "The job did not finish within " + cfg.stageJobTimeoutMs / 1000 + " s (last state " + state
                        + "), so it was cancelled. " + Objects.toString(flinkUnavailable(), "");
            } else {
                String cause = flink.rootCause(jobId);
                why = "The Flink job ended " + state + (cause == null ? "." : ": " + cause);
            }
            fail(s, why.trim(), null);
            h.finish("FAILED", s.error);
            return false;
        }
        return true;
    }

    private synchronized String ensureJar() throws Exception {
        String name = cfg.flinkJobJar.getFileName().toString();
        List<String> ids = flink.jarIds(name);
        if (jarId != null && ids.contains(jarId)) {
            return jarId;
        }
        if (!ids.isEmpty()) {
            jarId = ids.get(0);
            return jarId;
        }
        jarId = flink.uploadJar(cfg.flinkJobJar);
        LOG.info("Uploaded {} to Flink as {}", name, jarId);
        return jarId;
    }

    /** A user-facing reason the Flink cluster cannot run a job right now, or null. */
    private String flinkUnavailable() {
        try {
            JsonNode o = flink.overview();
            if (o.path("taskmanagers").asInt() == 0) {
                return "No Flink TaskManager is registered, so a job cannot run. Start it with "
                        + "`docker compose start flink-taskmanager` and retry.";
            }
            if (o.path("slots-available").asInt() == 0) {
                return "All " + o.path("slots-total").asInt() + " Flink task slots are busy (for example with running SQL "
                        + "Workspace statements). Cancel one and retry.";
            }
            return null;
        } catch (Exception e) {
            return "The Flink REST API is not reachable: " + root(e);
        }
    }

    private String flussUnavailable() {
        try {
            FlussGateway.Nodes n = fluss.serverNodes(Duration.ofSeconds(4));
            if (n.coordinators == 0 || n.tabletServers == 0) {
                return "Fluss has no " + (n.coordinators == 0 ? "coordinator" : "TabletServer")
                        + " registered, so nothing can be written. Start it with `docker compose start fluss-tablet` and retry.";
            }
            return null;
        } catch (Exception e) {
            return "Fluss is not reachable: " + root(e);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static Run.ResultView view(KafkaGateway.Rec rec, ComputedResults.Parsed p) {
        Run.ResultView v = new Run.ResultView();
        v.offset = rec.offset;
        v.key = rec.key;
        v.targetTable = p.targetTable;
        v.executionId = p.executionId;
        v.producer = p.producer;
        v.row = p.row;
        v.errors = p.errors.isEmpty() ? null : p.errors;
        return v;
    }

    /** Expected values as the lookup reports them: timestamps as ISO instants. */
    private static Map<String, Object> displayValues(ComputedResults.Parsed p) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (TableSchemas.Column c : p.spec.columns) {
            if (TableSchemas.PROVENANCE_COLUMNS.contains(c.name)) {
                continue;
            }
            Object v = p.values.get(c.name);
            if (v != null && c.type == TableSchemas.ColumnType.TIMESTAMP_LTZ_3) {
                v = Instant.ofEpochMilli((Long) v).toString();
            }
            m.put(c.name, v);
        }
        return m;
    }

    private static JsonNode parseOrNull(String s) {
        try {
            return s == null ? null : Json.mapper().readTree(s);
        } catch (Exception e) {
            return null;
        }
    }

    static String shortId(Run r) {
        return r.runId.startsWith("run-") ? r.runId.substring(4) : r.runId;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static String root(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        String name = t.getClass().getSimpleName();
        if (t.getMessage() == null || t.getMessage().isBlank()) {
            return name.endsWith("TimeoutException") ? name + " (no answer before the call's time limit)" : name;
        }
        return name + ": " + t.getMessage();
    }
}
