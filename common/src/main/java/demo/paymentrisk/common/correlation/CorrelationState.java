package demo.paymentrisk.common.correlation;

import java.util.Map;

/**
 * The keyed state the correlation engine needs, for one (run, supplier) key.
 *
 * <p>The Flink job implements this with keyed {@code ValueState}/{@code MapState} under a
 * processing-time TTL; unit tests use {@link InMemoryCorrelationState}.
 */
public interface CorrelationState {
    SupplierContext loadContext();

    void storeContext(SupplierContext context);

    boolean isEventSeen(String eventId);

    void markEventSeen(String eventId, long processingTimeMs);

    /** @return the event ID that produced the review for this payment, or null if none. */
    String reviewedPaymentEventId(String paymentId);

    void markPaymentReviewed(String paymentId, String eventId);

    /** Identity events waiting for their bank change, keyed by event ID. */
    Map<String, IdentityEvidence> pendingIdentities();

    void putPendingIdentity(IdentityEvidence evidence);

    void removePendingIdentity(String eventId);
}
