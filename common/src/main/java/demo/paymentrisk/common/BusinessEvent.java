package demo.paymentrisk.common;

/**
 * A parsed business event with its payload flattened into typed, nullable fields.
 *
 * <p>Public fields and a public no-arg constructor make this a valid Flink POJO, so it is
 * serialized efficiently between the Kafka source and the keyed correlation operator.
 * Times are UTC epoch milliseconds. Money is an integer amount of minor units (cents).
 */
public class BusinessEvent {

    // ---- envelope (common fields, see schemas/business-event-v1.schema.json) ----
    public String schemaVersion;
    public String runId;
    public String eventId;
    public String eventType;
    public String supplierId;
    /** Business time: when the fact happened (drives the rule). */
    public Long occurredAtMs;
    /** Wall-clock time the generator handed the event to the Kafka producer. */
    public Long emittedAtMs;
    public String correlationId;

    // ---- payload (which fields are set depends on eventType) ----
    public String sourceSystem;
    public String supplierName;
    public Integer relationshipYears;
    /** SUPPLIER_REGISTERED: established beneficiary. PAYMENT_REQUESTED: destination. */
    public String beneficiaryMasked;
    public String previousBeneficiaryMasked;
    public String newBeneficiaryMasked;
    public String changeId;
    public String actorId;
    public String sessionId;
    public String deviceId;
    public Boolean deviceKnown;
    public String activity;
    public String paymentId;
    public Long amountMinor;
    public String currency;

    // ---- transport metadata, filled in by the Kafka deserializer ----
    public Integer kafkaPartition;
    public Long kafkaOffset;
    public Long kafkaTimestampMs;

    /** Non-null when the record parsed but failed validation; it is then receipted as REJECTED_INVALID. */
    public String invalidReason;

    public BusinessEvent() {}

    public EventType type() {
        return EventType.parse(eventType);
    }

    /** Kafka record key and Flink key: one ordered stream of evidence per run and supplier. */
    public String correlationKey() {
        return correlationKey(runId, supplierId);
    }

    public static String correlationKey(String runId, String supplierId) {
        return runId + "|" + supplierId;
    }

    @Override
    public String toString() {
        return "BusinessEvent{" + eventType + " " + eventId + " run=" + runId + " supplier=" + supplierId
                + (invalidReason == null ? "" : " INVALID: " + invalidReason) + "}";
    }
}
