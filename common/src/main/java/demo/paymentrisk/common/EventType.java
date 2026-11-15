package demo.paymentrisk.common;

/** The four business event types carried on the single {@code business-events} topic. */
public enum EventType {
    /** Supplier master data baseline (simulated source: vendor master). */
    SUPPLIER_REGISTERED,
    /** Beneficiary bank-account change, shaped like a CDC update (simulated; no database is deployed). */
    BENEFICIARY_CHANGED,
    /** Identity-provider activity linked to a specific modification (simulated). */
    IDENTITY_ACTIVITY,
    /** A payment request that must be evaluated by the review rule (simulated). */
    PAYMENT_REQUESTED;

    public static EventType parse(String value) {
        if (value == null) {
            return null;
        }
        for (EventType t : values()) {
            if (t.name().equals(value)) {
                return t;
            }
        }
        return null;
    }
}
