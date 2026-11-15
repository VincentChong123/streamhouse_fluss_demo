package demo.paymentrisk.app.run;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import demo.paymentrisk.app.infra.FlussGateway;
import demo.paymentrisk.app.scenario.EventFactory;
import demo.paymentrisk.app.scenario.Scenario;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * One demo run: fresh identifiers plus the server-held state of the four stages. Everything a
 * browser shows comes from here, so a refresh or reconnect shows the actual progress and never
 * repeats an operation. Fields are public for JSON serialization (snake_case).
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public final class Run {
    public enum Status { NOT_STARTED, RUNNING, SUCCEEDED, FAILED }

    public final String runId;
    public final String scenarioId;
    public final String title;
    public final String summary;
    public final String supplierId;
    public final String supplierName;
    public final String paymentId;
    public final String correlationId;
    public final String createdAt;
    /** Business date of the scenario; the events happen at 10:00–10:03 UTC on this date. */
    public final String businessDate;
    /** Log-end offsets captured when the run was created: this run's records come after them. */
    public volatile Long inputStartOffset;
    public volatile Long computedStartOffset;

    public final Stage1 stage1 = new Stage1();
    public final Stage2 stage2 = new Stage2();
    public final Stage3 stage3 = new Stage3();
    public final Stage4 stage4 = new Stage4();

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Stage {
        public volatile Status status = Status.NOT_STARTED;
        public volatile String startedAt;
        public volatile String finishedAt;
        public volatile Long durationMs;
        /** What the operation is doing right now (only while RUNNING). */
        public volatile String phase;
        public volatile String error;
        public volatile String errorDetail;
        public volatile String warning;
        /** Computed when the snapshot is built. */
        public volatile boolean canRun;
        public volatile String blockedReason;
        public volatile int executions;
        /** Measured wall-clock time of each step of the latest execution, in order. */
        public final List<Step> steps = new CopyOnWriteArrayList<>();
        private volatile long stepStartNanos;

        void begin() {
            status = Status.RUNNING;
            startedAt = Instant.now().toString();
            finishedAt = null;
            durationMs = null;
            error = null;
            errorDetail = null;
            warning = null;
            phase = null;
            steps.clear();
            executions++;
        }

        void end(Status s) {
            closeStep();
            status = s;
            finishedAt = Instant.now().toString();
            durationMs = startedAt == null ? null : Instant.parse(finishedAt).toEpochMilli() - Instant.parse(startedAt).toEpochMilli();
            phase = null;
        }

        void reset() {
            status = Status.NOT_STARTED;
            startedAt = null;
            finishedAt = null;
            durationMs = null;
            phase = null;
            error = null;
            errorDetail = null;
            warning = null;
            steps.clear();
        }

        /** Ends the current step (if any) and starts timing a new one. */
        synchronized void step(String name) {
            closeStep();
            Step st = new Step();
            st.name = name;
            st.running = true;
            steps.add(st);
            stepStartNanos = System.nanoTime();
        }

        private synchronized void closeStep() {
            if (!steps.isEmpty()) {
                Step last = steps.get(steps.size() - 1);
                if (last.running) {
                    last.ms = (System.nanoTime() - stepStartNanos) / 1_000_000;
                    last.running = false;
                }
            }
        }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Step {
        public volatile String name;
        public volatile long ms;
        public volatile boolean running;
    }

    // ------------------------------------------------------------------ stage 1

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class EventView {
        public String step;
        public String label;
        public String caption;
        public String notice;
        public String eventType;
        public String eventId;
        public String occurredAt;
        public String key;
        /** The exact JSON value sent to Kafka (null until it is built). */
        public volatile JsonNode record;
        /** PENDING | SENDING | ACKNOWLEDGED | FAILED */
        public volatile String status = "PENDING";
        public volatile Integer partition;
        public volatile Long offset;
        public volatile String kafkaTimestamp;
        public volatile String ackedAt;
        public volatile Long sendMs;
        public volatile String error;
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Stage1 extends Stage {
        public String topic;
        public final List<EventView> events = new CopyOnWriteArrayList<>();
    }

    // ------------------------------------------------------------------ stage 2

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class JobExecution {
        public String executionId;
        public volatile String jobId;
        public volatile String jobName;
        public volatile String jobState;
        public volatile String jobUrl;
        public volatile String submittedAt;
        public volatile String jobFinishedAt;
        public volatile Long jobDurationMs;
        public volatile Map<String, String> accumulators;
        /** Arguments given to the job (offset range, run, execution). */
        public volatile List<String> programArgs;
    }

    /** One computed-result record read back from the computed-results topic. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class ResultView {
        public long offset;
        public String key;
        public String targetTable;
        public String executionId;
        public String producer;
        public JsonNode row;
        public List<String> errors;
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class ProcessingExecution extends JobExecution {
        public String inputTopic;
        public int inputPartition;
        public volatile Long inputStart;
        public volatile Long inputEnd;
        public volatile Integer inputRecordsThisRun;
        public volatile Integer inputRecordsOtherRuns;
        public volatile List<String> inputEventIds;
        public String outputTopic;
        public volatile Long outputStart;
        public volatile Long outputEnd;
        public volatile List<ResultView> results;
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Stage2 extends Stage {
        public volatile ProcessingExecution execution;
    }

    // ------------------------------------------------------------------ stage 3

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Rejected {
        public long offset;
        public String executionId;
        public String targetTable;
        public List<String> errors;
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Destination {
        public String table;
        public List<String> primaryKey;
        public int rowsExpected;
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class ReadBackRow {
        public String table;
        public Map<String, String> key;
        public String computedBy;
        public long sourceOffset;
        public Map<String, Object> expected;
        public volatile FlussGateway.Observation observation;
        public volatile boolean matches;
        public volatile List<String> mismatches;
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class ReadBack {
        public volatile boolean verified;
        public volatile int checked;
        public volatile int matched;
        public volatile String method;
        public final List<ReadBackRow> rows = new CopyOnWriteArrayList<>();
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class IngestionExecution extends JobExecution {
        public String sourceTopic;
        public int sourcePartition;
        public volatile Long sourceStart;
        public volatile Long sourceEnd;
        public volatile Integer recordsThisRun;
        public volatile Integer recordsOtherRuns;
        public volatile Integer recordsValid;
        public final List<Rejected> rejected = new CopyOnWriteArrayList<>();
        public volatile Map<String, Integer> byProducer;
        public final List<Destination> destinations = new CopyOnWriteArrayList<>();
        /** supplier_risk versions present in the source range, in offset order. */
        public volatile List<ResultView> supplierVersions;
        public final ReadBack readBack = new ReadBack();
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Stage3 extends Stage {
        public volatile IngestionExecution execution;
    }

    // ------------------------------------------------------------------ stage 4

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class QueryResult {
        public String queriedAt;
        public String mechanism;
        public FlussGateway.Observation supplier;
        public FlussGateway.Observation review;
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Stage4 extends Stage {
        public volatile QueryResult result;
    }

    // ------------------------------------------------------------------ construction

    Run(String runId, Scenario s, String inputTopic) {
        this.runId = runId;
        this.scenarioId = s.id;
        this.title = s.title;
        this.summary = s.summary;
        this.supplierId = s.supplierId;
        this.supplierName = s.steps.get(0).payload == null ? s.supplierId
                : s.steps.get(0).payload.path("supplier_name").asText(s.supplierId);
        this.paymentId = s.paymentId;
        this.correlationId = "corr-" + runId;
        this.createdAt = Instant.now().toString();
        this.businessDate = LocalDate.now(ZoneOffset.UTC).toString();
        this.stage1.topic = inputTopic;
        for (Scenario.Step st : s.steps) {
            EventView e = new EventView();
            e.step = st.id;
            e.label = st.label;
            e.caption = st.caption;
            e.notice = st.notice;
            e.eventType = st.eventType;
            e.eventId = EventFactory.newEventId();
            e.occurredAt = businessTime(st.businessOffsetSeconds).toString();
            e.key = demo.paymentrisk.common.BusinessEvent.correlationKey(runId, s.supplierId);
            stage1.events.add(e);
        }
    }

    /** 10:00:00 UTC on the business date, plus the step's offset. */
    public Instant businessTime(long offsetSeconds) {
        return Instant.parse(businessDate + "T10:00:00Z").plusSeconds(offsetSeconds);
    }
}
