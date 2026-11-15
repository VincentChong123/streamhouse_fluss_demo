package demo.paymentrisk.app.playground;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.app.AppConfig;
import demo.paymentrisk.app.infra.KafkaGateway;
import demo.paymentrisk.app.run.ExecutionHistory;
import demo.paymentrisk.app.run.Run;
import demo.paymentrisk.app.run.RunManager;
import demo.paymentrisk.app.scenario.EventFactory;
import demo.paymentrisk.common.BusinessEvent;
import demo.paymentrisk.common.EventCodec;
import demo.paymentrisk.common.Json;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manual producer. Records are validated before anything is sent: business-events must satisfy
 * the business-event v1 contract (the processing job depends on it); playground-events take any
 * JSON object. A batch is sent only if every record is valid.
 */
public final class EventsWorkspace {
    public static final int MAX_BATCH = 100;
    public static final int MAX_RECORD_BYTES = 64 * 1024;
    private static final int MAX_RECENT = 50;

    /** 400: nothing was sent. */
    public static final class InvalidRequest extends Exception {
        public final List<RecordError> errors;

        InvalidRequest(String message, List<RecordError> errors) {
            super(message);
            this.errors = errors;
        }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class RecordError {
        public Integer index;
        public List<String> errors;

        RecordError(Integer index, List<String> errors) {
            this.index = index;
            this.errors = errors;
        }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Sent {
        public String id;
        public String topic;
        public String key;
        public JsonNode value;
        public String runId;
        /** SENDING | ACKNOWLEDGED | FAILED */
        public volatile String status = "SENDING";
        public volatile Integer partition;
        public volatile Long offset;
        public volatile String kafkaTimestamp;
        public volatile Long sendMs;
        public volatile String error;
        public String warning;
        public String sentAt;
    }

    private final AppConfig cfg;
    private final KafkaGateway kafka;
    private final RunManager runs;
    private final ExecutionHistory history;
    private final Runnable onChange;
    private final List<Sent> recent = new CopyOnWriteArrayList<>();

    public EventsWorkspace(AppConfig cfg, KafkaGateway kafka, RunManager runs, ExecutionHistory history, Runnable onChange) {
        this.cfg = cfg;
        this.kafka = kafka;
        this.runs = runs;
        this.history = history;
        this.onChange = onChange;
    }

    public List<Sent> recent() {
        return new ArrayList<>(recent);
    }

    // ------------------------------------------------------------------ templates

    /** Event templates for the current run, with fresh IDs (not additional business scenarios). */
    public ArrayNode templates() {
        Run r = runs.current();
        String runId = r == null ? "run-unknown" : r.runId;
        String supplier = r == null ? "SUP-NORTHSTAR" : r.supplierId;
        String corr = r == null ? "corr-unknown" : r.correlationId;
        Instant base = r == null ? Instant.now() : r.businessTime(0);
        ArrayNode out = Json.mapper().createArrayNode();

        ObjectNode simple = Json.mapper().createObjectNode();
        simple.put("event_id", "pg-" + UUID.randomUUID().toString().substring(0, 8));
        simple.put("category", "invoice");
        simple.put("amount", 1250.50);
        simple.put("currency", "USD");
        simple.put("created_at", Instant.now().toString());
        add(out, "simple", "Simple JSON event", cfg.playgroundTopic, "invoice", simple,
                "Any JSON object. Query it in the SQL Workspace through the playground_events table.");

        ObjectNode baseline = business(runId, supplier, corr, "SUPPLIER_REGISTERED", base.plusSeconds(240));
        baseline.with("payload").put("source_system", "manual-entry").put("supplier_name", "Northstar Logistics")
                .put("relationship_years", 3).put("beneficiary_masked", "•••• 1842");
        add(out, "baseline", "Supplier baseline", cfg.inputTopic, BusinessEvent.correlationKey(runId, supplier), baseline,
                "SUPPLIER_REGISTERED: creates or refreshes the supplier context.");

        ObjectNode change = business(runId, supplier, corr, "BENEFICIARY_CHANGED", base.plusSeconds(240));
        ObjectNode cp = change.with("payload");
        cp.put("source_system", "manual-entry").put("change_id", "CHG-2001").put("actor_id", "usr-ap-clerk-17")
                .put("session_id", "sess-9b1c");
        cp.putObject("before").put("beneficiary_masked", "•••• 9077");
        cp.putObject("after").put("beneficiary_masked", "•••• 5521");
        add(out, "bank-change", "Beneficiary change", cfg.inputTopic, BusinessEvent.correlationKey(runId, supplier), change,
                "BENEFICIARY_CHANGED: change_id, actor_id and session_id link later identity activity to this change.");

        ObjectNode identity = business(runId, supplier, corr, "IDENTITY_ACTIVITY", base.plusSeconds(270));
        identity.with("payload").put("source_system", "manual-entry").put("change_id", "CHG-2001")
                .put("actor_id", "usr-ap-clerk-17").put("session_id", "sess-9b1c").put("device_id", "dev-known-laptop-03")
                .put("device_is_known", true).put("activity", "BANK_DETAILS_MODIFIED");
        add(out, "identity", "Identity activity", cfg.inputTopic, BusinessEvent.correlationKey(runId, supplier), identity,
                "IDENTITY_ACTIVITY: matched to a change only when change_id, actor_id and session_id all agree.");

        ObjectNode payment = business(runId, supplier, corr, "PAYMENT_REQUESTED", base.plusSeconds(300));
        payment.with("payload").put("source_system", "manual-entry").put("payment_id", "PAY-NS-" + (1000 + (int) (Math.random() * 9000)))
                .put("amount_minor", 7_500_000L).put("currency", "USD").put("beneficiary_masked", "•••• 9077");
        add(out, "payment", "Payment request", cfg.inputTopic, BusinessEvent.correlationKey(runId, supplier), payment,
                "PAYMENT_REQUESTED: amount_minor is in cents (7500000 = USD 75,000). Evaluated once per payment_id.");
        return out;
    }

    private static ObjectNode business(String runId, String supplier, String corr, String type, Instant occurredAt) {
        ObjectNode e = Json.mapper().createObjectNode();
        e.put("schema_version", EventCodec.SCHEMA_VERSION);
        e.put("run_id", runId);
        e.put("event_id", EventFactory.newEventId());
        e.put("event_type", type);
        e.put("supplier_id", supplier);
        e.put("occurred_at", occurredAt.toString());
        e.put("emitted_at", Instant.now().toString());
        e.put("correlation_id", corr);
        e.putObject("payload");
        return e;
    }

    private static void add(ArrayNode out, String id, String label, String topic, String key, ObjectNode value, String help) {
        ObjectNode t = out.addObject();
        t.put("id", id);
        t.put("label", label);
        t.put("topic", topic);
        t.put("key", key);
        t.set("value", value);
        t.put("description", help);
    }

    // ------------------------------------------------------------------ send

    /** Validates everything first; sends only if all records are valid. Returns the sent records. */
    public List<Sent> send(String topic, String key, String text, boolean batch) throws InvalidRequest {
        boolean business = cfg.inputTopic.equals(topic);
        if (!business && !cfg.playgroundTopic.equals(topic)) {
            throw new InvalidRequest("Topic must be " + cfg.inputTopic + " or " + cfg.playgroundTopic + ".", List.of());
        }
        if (text == null || text.isBlank()) {
            throw new InvalidRequest("The JSON payload is empty.", List.of());
        }
        List<JsonNode> values = parse(text, batch);
        if (values.size() > MAX_BATCH) {
            throw new InvalidRequest("A batch holds at most " + MAX_BATCH + " records (found " + values.size() + ").", List.of());
        }
        Run current = runs.current();
        List<RecordError> errors = new ArrayList<>();
        List<Sent> plan = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            JsonNode v = values.get(i);
            List<String> errs = new ArrayList<>();
            String json = Json.write(v);
            if (json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_RECORD_BYTES) {
                errs.add("record is larger than " + MAX_RECORD_BYTES / 1024 + " KiB");
            }
            Sent s = new Sent();
            s.topic = topic;
            s.value = v;
            if (!v.isObject()) {
                errs.add("each record must be a JSON object (found " + v.getNodeType().name().toLowerCase() + ")");
            } else if (business) {
                BusinessEvent e = EventCodec.parse(json);
                if (e == null) {
                    errs.add("business events need run_id, event_id and supplier_id (strings); see the templates");
                } else {
                    if (e.invalidReason != null) {
                        errs.addAll(Arrays.asList(e.invalidReason.split("; ")));
                    }
                    String expectedKey = e.correlationKey();
                    String k = batch || key == null || key.isBlank() ? expectedKey : key.trim();
                    if (!expectedKey.equals(k)) {
                        errs.add("key must be run_id|supplier_id (" + expectedKey + ") so one supplier's events stay in order");
                    }
                    s.key = expectedKey;
                    s.runId = e.runId;
                    if (current != null && !current.runId.equals(e.runId)) {
                        s.warning = "run_id " + e.runId + " is not the current run (" + current.runId
                                + "); the current run's jobs ignore this record.";
                    }
                }
            } else {
                s.key = key == null || key.isBlank() ? null : key.trim();
            }
            if (!errs.isEmpty()) {
                errors.add(new RecordError(values.size() > 1 || batch ? i : null, errs));
            }
            plan.add(s);
        }
        if (!errors.isEmpty()) {
            throw new InvalidRequest(errors.size() == 1 && values.size() == 1 ? "The record was not sent: it does not match the "
                    + (business ? "business-event v1 contract." : "playground format.")
                    : errors.size() + " of " + values.size() + " records are invalid; nothing was sent.", errors);
        }

        boolean forCurrentRun = current != null && plan.stream().anyMatch(s -> current.runId.equals(s.runId));
        if (forCurrentRun) {
            try {
                runs.captureOffsets(current); // the run's range must start before these records
            } catch (Exception e) {
                throw new InvalidRequest("Kafka is not reachable; nothing was sent: " + RunManager.root(e), List.of());
            }
        }
        ExecutionHistory.Entry h = history.add("send-" + UUID.randomUUID().toString().substring(0, 8), "EVENT_SEND",
                "Send " + plan.size() + " record(s) to " + topic, current == null ? null : current.runId);
        int acked = 0;
        String failure = null;
        for (Sent s : plan) {
            s.id = "rec-" + UUID.randomUUID().toString().substring(0, 8);
            s.sentAt = Instant.now().toString();
            recent.add(0, s);
            while (recent.size() > MAX_RECENT) {
                recent.remove(recent.size() - 1);
            }
            if (failure != null) {
                s.status = "FAILED";
                s.error = "not sent: an earlier record in this batch failed";
                continue;
            }
            onChange.run();
            try {
                KafkaGateway.Ack ack = kafka.send(topic, s.key, Json.write(s.value), Duration.ofMillis(cfg.sendTimeoutMs));
                s.partition = ack.partition;
                s.offset = ack.offset;
                s.kafkaTimestamp = Instant.ofEpochMilli(ack.kafkaTimestampMs).toString();
                s.sendMs = ack.ackedAtMs - ack.sendStartedMs;
                s.status = "ACKNOWLEDGED";
                acked++;
            } catch (Exception e) {
                s.status = "FAILED";
                s.error = RunManager.root(e);
                failure = s.error;
            }
        }
        h.finish(failure == null ? "SUCCEEDED" : "FAILED", acked + " of " + plan.size() + " acknowledged"
                + (failure == null ? "" : "; " + failure));
        onChange.run();
        return plan;
    }

    private static List<JsonNode> parse(String text, boolean batch) throws InvalidRequest {
        String t = text.trim();
        List<JsonNode> out = new ArrayList<>();
        if (!batch) {
            JsonNode n = readOne(t, null);
            if (n.isArray()) {
                throw new InvalidRequest("The payload is a JSON array. Enable batch mode to send each element as a record.", List.of());
            }
            out.add(n);
            return out;
        }
        if (t.startsWith("[")) {
            JsonNode n = readOne(t, null);
            n.forEach(out::add);
        } else {
            String[] lines = t.split("\\r?\\n");
            for (int i = 0; i < lines.length; i++) {
                if (!lines[i].isBlank()) {
                    out.add(readOne(lines[i], i + 1));
                }
            }
        }
        if (out.isEmpty()) {
            throw new InvalidRequest("The batch is empty.", List.of());
        }
        return out;
    }

    private static JsonNode readOne(String text, Integer lineNo) throws InvalidRequest {
        try {
            JsonNode n = Json.mapper().readTree(text);
            if (n == null || n.isMissingNode()) {
                throw new InvalidRequest("The JSON payload is empty.", List.of());
            }
            return n;
        } catch (JsonProcessingException e) {
            JsonLocation loc = e.getLocation();
            String where = lineNo != null ? "line " + lineNo : loc == null ? "" : "line " + loc.getLineNr() + ", column " + loc.getColumnNr();
            throw new InvalidRequest("Invalid JSON" + (where.isEmpty() ? "" : " at " + where) + ": " + e.getOriginalMessage()
                    + (lineNo != null ? " (batch mode expects a JSON array or one JSON object per line)" : ""), List.of());
        }
    }
}
