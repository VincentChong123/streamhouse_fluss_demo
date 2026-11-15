package demo.paymentrisk.common.correlation;

import demo.paymentrisk.common.Json;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Plain-Java state for unit tests. Contexts are copied through JSON, as Flink state would be. */
public class InMemoryCorrelationState implements CorrelationState {
    private String contextJson;
    private final Map<String, Long> seen = new HashMap<>();
    private final Map<String, String> reviewed = new HashMap<>();
    private final Map<String, IdentityEvidence> pending = new LinkedHashMap<>();

    @Override
    public SupplierContext loadContext() {
        return contextJson == null ? null : Json.read(contextJson, SupplierContext.class);
    }

    @Override
    public void storeContext(SupplierContext context) {
        contextJson = Json.write(context);
    }

    @Override
    public boolean isEventSeen(String eventId) {
        return seen.containsKey(eventId);
    }

    @Override
    public void markEventSeen(String eventId, long processingTimeMs) {
        seen.put(eventId, processingTimeMs);
    }

    @Override
    public String reviewedPaymentEventId(String paymentId) {
        return reviewed.get(paymentId);
    }

    @Override
    public void markPaymentReviewed(String paymentId, String eventId) {
        reviewed.put(paymentId, eventId);
    }

    @Override
    public Map<String, IdentityEvidence> pendingIdentities() {
        Map<String, IdentityEvidence> copy = new LinkedHashMap<>();
        pending.forEach((k, v) -> copy.put(k, Json.read(Json.write(v), IdentityEvidence.class)));
        return copy;
    }

    @Override
    public void putPendingIdentity(IdentityEvidence evidence) {
        pending.put(evidence.eventId, evidence);
    }

    @Override
    public void removePendingIdentity(String eventId) {
        pending.remove(eventId);
    }
}
