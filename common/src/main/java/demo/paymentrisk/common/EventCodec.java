package demo.paymentrisk.common;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Parses and validates business-event JSON (schema version 1).
 *
 * <p>Validation mirrors {@code schemas/business-event-v1.schema.json}. A record whose envelope
 * identifies a run, event and supplier but is otherwise invalid is returned with
 * {@link BusinessEvent#invalidReason} set, so the pipeline can store a visible REJECTED receipt
 * instead of silently dropping it. A record that cannot be identified at all yields {@code null}.
 */
public final class EventCodec {
    public static final String SCHEMA_VERSION = "1";
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");

    private EventCodec() {}

    public static BusinessEvent parse(String json) {
        JsonNode root;
        try {
            root = Json.mapper().readTree(json);
        } catch (Exception e) {
            return null;
        }
        if (root == null || !root.isObject()) {
            return null;
        }
        BusinessEvent e = new BusinessEvent();
        e.runId = text(root, "run_id");
        e.eventId = text(root, "event_id");
        e.supplierId = text(root, "supplier_id");
        if (isBlank(e.runId) || isBlank(e.eventId) || isBlank(e.supplierId)) {
            return null;
        }
        List<String> errors = new ArrayList<>();
        e.schemaVersion = text(root, "schema_version");
        if (!SCHEMA_VERSION.equals(e.schemaVersion)) {
            errors.add("schema_version must be \"1\"");
        }
        e.eventType = text(root, "event_type");
        EventType type = EventType.parse(e.eventType);
        if (type == null) {
            errors.add("unknown event_type " + e.eventType);
        }
        e.occurredAtMs = instant(root, "occurred_at", errors);
        e.emittedAtMs = instant(root, "emitted_at", errors);
        e.correlationId = text(root, "correlation_id");
        if (isBlank(e.correlationId)) {
            errors.add("correlation_id is required");
        }
        JsonNode p = root.get("payload");
        if (p == null || !p.isObject()) {
            errors.add("payload object is required");
        } else if (type != null) {
            e.sourceSystem = text(p, "source_system");
            switch (type) {
                case SUPPLIER_REGISTERED:
                    e.supplierName = required(p, "supplier_name", errors);
                    e.relationshipYears = nonNegativeInt(p, "relationship_years", errors);
                    e.beneficiaryMasked = required(p, "beneficiary_masked", errors);
                    break;
                case BENEFICIARY_CHANGED:
                    e.changeId = required(p, "change_id", errors);
                    e.actorId = required(p, "actor_id", errors);
                    e.sessionId = required(p, "session_id", errors);
                    e.previousBeneficiaryMasked = nested(p, "before", "beneficiary_masked", errors);
                    e.newBeneficiaryMasked = nested(p, "after", "beneficiary_masked", errors);
                    break;
                case IDENTITY_ACTIVITY:
                    e.changeId = required(p, "change_id", errors);
                    e.actorId = required(p, "actor_id", errors);
                    e.sessionId = required(p, "session_id", errors);
                    e.deviceId = required(p, "device_id", errors);
                    JsonNode known = p.get("device_is_known");
                    if (known == null || !known.isBoolean()) {
                        errors.add("payload.device_is_known must be a boolean");
                    } else {
                        e.deviceKnown = known.booleanValue();
                    }
                    e.activity = text(p, "activity");
                    break;
                case PAYMENT_REQUESTED:
                    e.paymentId = required(p, "payment_id", errors);
                    JsonNode amount = p.get("amount_minor");
                    if (amount == null || !amount.isIntegralNumber() || !amount.canConvertToLong() || amount.longValue() < 0) {
                        errors.add("payload.amount_minor must be a non-negative integer number of minor units");
                    } else {
                        e.amountMinor = amount.longValue();
                    }
                    e.currency = required(p, "currency", errors);
                    if (e.currency != null && !CURRENCY.matcher(e.currency).matches()) {
                        errors.add("payload.currency must be an ISO-4217 code such as USD");
                    }
                    e.beneficiaryMasked = text(p, "beneficiary_masked");
                    break;
                default:
                    break;
            }
        }
        if (!errors.isEmpty()) {
            e.invalidReason = String.join("; ", errors);
        }
        return e;
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() || !v.isTextual() ? null : v.textValue();
    }

    private static String required(JsonNode n, String field, List<String> errors) {
        String v = text(n, field);
        if (isBlank(v)) {
            errors.add("payload." + field + " is required");
        }
        return v;
    }

    private static String nested(JsonNode n, String object, String field, List<String> errors) {
        JsonNode o = n.get(object);
        String v = o == null ? null : text(o, field);
        if (isBlank(v)) {
            errors.add("payload." + object + "." + field + " is required");
        }
        return v;
    }

    private static Integer nonNegativeInt(JsonNode n, String field, List<String> errors) {
        JsonNode v = n.get(field);
        if (v == null || !v.isIntegralNumber() || !v.canConvertToInt() || v.intValue() < 0) {
            errors.add("payload." + field + " must be a non-negative integer");
            return null;
        }
        return v.intValue();
    }

    private static Long instant(JsonNode n, String field, List<String> errors) {
        String v = text(n, field);
        if (v == null) {
            errors.add(field + " is required (ISO-8601 UTC instant)");
            return null;
        }
        try {
            return Instant.parse(v).toEpochMilli();
        } catch (DateTimeParseException ex) {
            errors.add(field + " is not an ISO-8601 instant: " + v);
            return null;
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
