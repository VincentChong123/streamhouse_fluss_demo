package demo.paymentrisk.common.correlation;

import demo.paymentrisk.common.BusinessEvent;
import demo.paymentrisk.common.EventCodec;
import demo.paymentrisk.common.OutputRow;
import demo.paymentrisk.common.rule.RuleConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Correlation behaviour with in-memory state: versions, buffering, dedup, run isolation. */
class CorrelationEngineTest {
    private static final Instant T0 = Instant.parse("2026-11-12T10:00:00Z");
    private final CorrelationEngine engine = new CorrelationEngine(RuleConfig.defaults());
    private long offset;

    private BusinessEvent parse(String json) {
        BusinessEvent e = EventCodec.parse(json);
        assertNotNull(e);
        e.kafkaPartition = 0;
        e.kafkaOffset = offset++;
        return e;
    }

    private static String envelope(String eventId, String type, int minute, String payload) {
        return "{\"schema_version\":\"1\",\"run_id\":\"run-a\",\"event_id\":\"" + eventId + "\",\"event_type\":\"" + type
                + "\",\"supplier_id\":\"SUP-NORTHSTAR\",\"occurred_at\":\"" + T0.plusSeconds(60L * minute)
                + "\",\"emitted_at\":\"" + T0 + "\",\"correlation_id\":\"corr-1\",\"payload\":" + payload + "}";
    }

    private BusinessEvent baseline(String id) {
        return parse(envelope(id, "SUPPLIER_REGISTERED", 0,
                "{\"supplier_name\":\"Northstar Logistics\",\"relationship_years\":3,\"beneficiary_masked\":\"•••• 1842\"}"));
    }

    private BusinessEvent change(String id, String changeId, int minute) {
        return parse(envelope(id, "BENEFICIARY_CHANGED", minute,
                "{\"change_id\":\"" + changeId + "\",\"actor_id\":\"u-17\",\"session_id\":\"s-9\","
                        + "\"before\":{\"beneficiary_masked\":\"•••• 1842\"},\"after\":{\"beneficiary_masked\":\"•••• 9077\"}}"));
    }

    private BusinessEvent identity(String id, String changeId, boolean known, int minute) {
        return parse(envelope(id, "IDENTITY_ACTIVITY", minute,
                "{\"change_id\":\"" + changeId + "\",\"actor_id\":\"u-17\",\"session_id\":\"s-9\",\"device_id\":\"dev-new\","
                        + "\"device_is_known\":" + known + "}"));
    }

    private BusinessEvent payment(String id, String paymentId, long amount, int minute) {
        return parse(envelope(id, "PAYMENT_REQUESTED", minute,
                "{\"payment_id\":\"" + paymentId + "\",\"amount_minor\":" + amount + ",\"currency\":\"USD\"}"));
    }

    private static List<OutputRow> rows(List<OutputRow> out, String table) {
        return out.stream().filter(r -> r.table().name.equals(table)).collect(Collectors.toList());
    }

    @Test
    void northstarVersionsOneTwoThreeThenManualReview() {
        InMemoryCorrelationState s = new InMemoryCorrelationState();
        List<OutputRow> o1 = engine.process(baseline("e1"), s, 1);
        assertEquals(1, rows(o1, "supplier_risk").get(0).get("context_version"));
        assertEquals("NORMAL", rows(o1, "supplier_risk").get(0).get("signal_band"));

        List<OutputRow> o2 = engine.process(change("e2", "CHG-1001", 1), s, 2);
        assertEquals(2, rows(o2, "supplier_risk").get(0).get("context_version"));
        assertEquals("ELEVATED", rows(o2, "supplier_risk").get(0).get("signal_band"));
        assertEquals("•••• 9077", rows(o2, "supplier_risk").get(0).get("current_beneficiary_masked"));

        List<OutputRow> o3 = engine.process(identity("e3", "CHG-1001", false, 2), s, 3);
        OutputRow sr = rows(o3, "supplier_risk").get(0);
        assertEquals(3, sr.get("context_version"));
        assertEquals("HIGH", sr.get("signal_band"));
        assertEquals(Boolean.TRUE, sr.get("unfamiliar_device_evidence"));

        List<OutputRow> o4 = engine.process(payment("e4", "PAY-1", 7_500_000L, 3), s, 4);
        OutputRow review = rows(o4, "payment_reviews").get(0);
        assertEquals("MANUAL_REVIEW", review.get("outcome"));
        assertEquals(3, review.get("context_version_used"));
        assertEquals("e4", review.get("source_payment_event_id"));
        assertEquals("REVIEWED", rows(o4, "event_receipts").get(0).get("disposition"));
    }

    @Test
    void duplicateEventIdProducesNoOutputAndNoVersionBump() {
        InMemoryCorrelationState s = new InMemoryCorrelationState();
        engine.process(baseline("e1"), s, 1);
        assertTrue(engine.process(baseline("e1"), s, 2).isEmpty());
        List<OutputRow> o = engine.process(change("e2", "CHG-1001", 1), s, 3);
        assertEquals(2, rows(o, "supplier_risk").get(0).get("context_version"));
    }

    @Test
    void replayedPaymentIdDoesNotCreateASecondReview() {
        InMemoryCorrelationState s = new InMemoryCorrelationState();
        engine.process(baseline("e1"), s, 1);
        engine.process(payment("e2", "PAY-1", 100L, 1), s, 2);
        List<OutputRow> o = engine.process(payment("e3", "PAY-1", 7_500_000L, 2), s, 3);
        assertTrue(rows(o, "payment_reviews").isEmpty());
        assertEquals("DUPLICATE_PAYMENT_IGNORED", rows(o, "event_receipts").get(0).get("disposition"));
    }

    @Test
    void identityBeforeChangeIsBufferedThenApplied() {
        InMemoryCorrelationState s = new InMemoryCorrelationState();
        engine.process(baseline("e1"), s, 1);
        List<OutputRow> early = engine.process(identity("e3", "CHG-1001", false, 2), s, 2);
        assertEquals("BUFFERED_AWAITING_CHANGE", rows(early, "event_receipts").get(0).get("disposition"));
        assertTrue(rows(early, "supplier_risk").isEmpty());

        List<OutputRow> o = engine.process(change("e2", "CHG-1001", 1), s, 3);
        List<OutputRow> supplier = rows(o, "supplier_risk");
        assertEquals(2, supplier.size()); // v2 (change) then v3 (buffered identity applied)
        assertEquals(3, supplier.get(1).get("context_version"));
        assertEquals("HIGH", supplier.get(1).get("signal_band"));
        List<OutputRow> receipts = rows(o, "event_receipts");
        assertEquals("APPLIED_FROM_BUFFER", receipts.get(1).get("disposition"));
        assertEquals("e3", receipts.get(1).get("event_id"));
        assertEquals("run-a", receipts.get(1).get("run_id"));

        OutputRow review = rows(engine.process(payment("e4", "PAY-1", 7_500_000L, 3), s, 4), "payment_reviews").get(0);
        assertEquals("MANUAL_REVIEW", review.get("outcome"));
    }

    @Test
    void unfamiliarDeviceForAnotherChangeDoesNotTriggerTheRule() {
        InMemoryCorrelationState s = new InMemoryCorrelationState();
        engine.process(baseline("e1"), s, 1);
        engine.process(change("e2", "CHG-1001", 1), s, 2);
        List<OutputRow> o = engine.process(identity("e3", "CHG-7731", false, 2), s, 3);
        assertEquals("BUFFERED_AWAITING_CHANGE", rows(o, "event_receipts").get(0).get("disposition"));
        OutputRow review = rows(engine.process(payment("e4", "PAY-1", 7_500_000L, 3), s, 4), "payment_reviews").get(0);
        assertEquals("REVIEW_INCOMPLETE_CONTEXT", review.get("outcome"));
        assertEquals("UNKNOWN", review.get("check_unfamiliar_device"));
    }

    @Test
    void invalidEventGetsARejectedReceipt() {
        InMemoryCorrelationState s = new InMemoryCorrelationState();
        BusinessEvent bad = parse(envelope("e9", "PAYMENT_REQUESTED", 0,
                "{\"payment_id\":\"PAY-9\",\"amount_minor\":75000.5,\"currency\":\"USD\"}"));
        assertNotNull(bad.invalidReason);
        List<OutputRow> o = engine.process(bad, s, 1);
        assertEquals("REJECTED_INVALID", rows(o, "event_receipts").get(0).get("disposition"));
        assertTrue(rows(o, "payment_reviews").isEmpty());
    }

    @Test
    void separateStateMeansSeparateRuns() {
        InMemoryCorrelationState runA = new InMemoryCorrelationState();
        InMemoryCorrelationState runB = new InMemoryCorrelationState();
        engine.process(baseline("e1"), runA, 1);
        engine.process(change("e2", "CHG-1001", 1), runA, 2);
        List<OutputRow> o = engine.process(baseline("e1-b"), runB, 3);
        assertEquals(1, rows(o, "supplier_risk").get(0).get("context_version"));
    }
}
