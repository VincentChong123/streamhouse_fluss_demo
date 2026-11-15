package demo.paymentrisk.app.scenario;

import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.common.EventCodec;
import demo.paymentrisk.common.Json;

import java.time.Instant;
import java.util.UUID;

/**
 * Turns a fixture step into a schema-v1 business event. Business times are anchored so the
 * final step lands near "now" ({@code occurred_at = anchor + business_offset_seconds}); the
 * replay of those business minutes happens over a few wall-clock seconds.
 */
public final class EventFactory {
    private EventFactory() {}

    public static String newEventId() {
        return "evt-" + UUID.randomUUID();
    }

    public static ObjectNode build(String runId, String correlationId, String supplierId, Scenario.Step step,
                                   String eventId, Instant occurredAt, Instant emittedAt) {
        ObjectNode e = Json.mapper().createObjectNode();
        e.put("schema_version", EventCodec.SCHEMA_VERSION);
        e.put("run_id", runId);
        e.put("event_id", eventId);
        e.put("event_type", step.eventType);
        e.put("supplier_id", supplierId);
        e.put("occurred_at", occurredAt.toString());
        e.put("emitted_at", emittedAt.toString());
        e.put("correlation_id", correlationId);
        e.set("payload", step.payload == null ? Json.mapper().createObjectNode() : step.payload.deepCopy());
        return e;
    }
}
