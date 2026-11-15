package demo.paymentrisk.common.correlation;

/** What the Flink job did with one consumed event (stored in {@code event_receipts}). */
public enum Disposition {
    /** Supplier context updated; a new context version was written. */
    APPLIED,
    /** Identity activity that arrived before its bank change, applied once the change arrived. */
    APPLIED_FROM_BUFFER,
    /** Identity activity whose bank change has not been seen yet; held in a bounded buffer. */
    BUFFERED_AWAITING_CHANGE,
    /** Identity activity for the current change but a different actor or session. */
    IGNORED_ACTOR_MISMATCH,
    /** Payment request evaluated; one row written to payment_reviews. */
    REVIEWED,
    /** A payment ID that was already reviewed; no second logical review is created. */
    DUPLICATE_PAYMENT_IGNORED,
    /** The event failed schema validation. */
    REJECTED_INVALID
}
