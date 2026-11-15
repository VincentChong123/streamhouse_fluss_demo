package demo.paymentrisk.common.rule;

import demo.paymentrisk.common.BusinessEvent;
import demo.paymentrisk.common.correlation.BankChange;
import demo.paymentrisk.common.correlation.IdentityEvidence;
import demo.paymentrisk.common.correlation.SupplierContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pure unit tests of the rule: no Kafka, Flink or Fluss involved. */
class PaymentReviewRuleTest {
    private static final long T0 = 1_800_000_000_000L; // arbitrary fixed business time
    private static final long MIN = 60_000L;
    private final RuleConfig cfg = RuleConfig.defaults();

    private static SupplierContext context(boolean withChange, Boolean deviceKnown, long changeAt, long identityAt) {
        SupplierContext c = SupplierContext.empty("run-1", "SUP-1");
        c.baselineKnown = true;
        c.baselineEventId = "evt-base";
        c.version = 1;
        if (withChange) {
            BankChange b = new BankChange();
            b.changeId = "CHG-1";
            b.eventId = "evt-chg";
            b.actorId = "actor-1";
            b.sessionId = "sess-1";
            b.beforeMasked = "•••• 1842";
            b.afterMasked = "•••• 9077";
            b.occurredAtMs = changeAt;
            c.bankChange = b;
            c.version = 2;
            if (deviceKnown != null) {
                IdentityEvidence ev = new IdentityEvidence();
                ev.eventId = "evt-id";
                ev.changeId = "CHG-1";
                ev.actorId = "actor-1";
                ev.sessionId = "sess-1";
                ev.deviceId = "dev-x";
                ev.deviceKnown = deviceKnown;
                ev.occurredAtMs = identityAt;
                c.matchedIdentities.add(ev);
                c.version = 3;
            }
        }
        return c;
    }

    private static BusinessEvent payment(long at, long amountMinor, String currency) {
        BusinessEvent p = new BusinessEvent();
        p.runId = "run-1";
        p.supplierId = "SUP-1";
        p.eventId = "evt-pay";
        p.paymentId = "PAY-1";
        p.occurredAtMs = at;
        p.amountMinor = amountMinor;
        p.currency = currency;
        return p;
    }

    @Test
    void northstarScenarioRequiresManualReview() {
        SupplierContext c = context(true, false, T0 + MIN, T0 + 2 * MIN);
        ReviewDecision d = PaymentReviewRule.evaluate(c, payment(T0 + 3 * MIN, 7_500_000L, "USD"), cfg);
        assertEquals(Outcome.MANUAL_REVIEW, d.outcome);
        assertEquals(TriState.TRUE, d.recentChange);
        assertEquals(TriState.TRUE, d.unfamiliarDevice);
        assertEquals(TriState.TRUE, d.largePayment);
        assertEquals(3, d.contextVersionUsed);
        assertEquals(120L, d.secondsSinceBankChange);
        assertEquals("evt-chg", d.bankChangeEventId);
        assertEquals("evt-id", d.identityEventId);
        assertNull(d.missingEvidence);
        assertTrue(d.explanation.contains("does not authorize"));
    }

    @Test
    void knownDeviceDoesNotMatch() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, true, T0, T0 + MIN), payment(T0 + 2 * MIN, 7_500_000L, "USD"), cfg);
        assertEquals(Outcome.NO_RULE_MATCH, d.outcome);
        assertEquals(TriState.FALSE, d.unfamiliarDevice);
    }

    @Test
    void exactlyFiftyThousandUsdIsNotLarge() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, false, T0, T0 + MIN), payment(T0 + 2 * MIN, 5_000_000L, "USD"), cfg);
        assertEquals(TriState.FALSE, d.largePayment);
        assertEquals(Outcome.NO_RULE_MATCH, d.outcome);
    }

    @Test
    void oneCentAboveThresholdIsLarge() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, false, T0, T0 + MIN), payment(T0 + 2 * MIN, 5_000_001L, "USD"), cfg);
        assertEquals(Outcome.MANUAL_REVIEW, d.outcome);
    }

    @Test
    void otherCurrencyIsNotLarge() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, false, T0, T0 + MIN), payment(T0 + 2 * MIN, 7_500_000L, "EUR"), cfg);
        assertEquals(TriState.FALSE, d.largePayment);
        assertEquals(Outcome.NO_RULE_MATCH, d.outcome);
    }

    @Test
    void windowBoundaryIsInclusiveAtExactlyTenMinutes() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, false, T0, T0 + MIN), payment(T0 + 10 * MIN, 7_500_000L, "USD"), cfg);
        assertEquals(TriState.TRUE, d.recentChange);
        assertEquals(Outcome.MANUAL_REVIEW, d.outcome);
    }

    @Test
    void changeOlderThanTenMinutesDoesNotMatch() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, false, T0, T0 + MIN), payment(T0 + 10 * MIN + 1, 7_500_000L, "USD"), cfg);
        assertEquals(TriState.FALSE, d.recentChange);
        assertEquals(Outcome.NO_RULE_MATCH, d.outcome);
    }

    @Test
    void changeAfterThePaymentIsNotRecent() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, false, T0 + 5 * MIN, T0 + 5 * MIN), payment(T0, 7_500_000L, "USD"), cfg);
        assertEquals(TriState.FALSE, d.recentChange);
    }

    @Test
    void identityAfterThePaymentIsNotEvidence() {
        SupplierContext c = context(true, false, T0, T0 + 5 * MIN);
        ReviewDecision d = PaymentReviewRule.evaluate(c, payment(T0 + 2 * MIN, 7_500_000L, "USD"), cfg);
        assertEquals(TriState.UNKNOWN, d.unfamiliarDevice);
        assertEquals(Outcome.REVIEW_INCOMPLETE_CONTEXT, d.outcome);
    }

    @Test
    void missingIdentityEvidenceIsIncompleteNotAPass() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, null, T0, 0), payment(T0 + MIN, 7_500_000L, "USD"), cfg);
        assertEquals(Outcome.REVIEW_INCOMPLETE_CONTEXT, d.outcome);
        assertEquals(TriState.UNKNOWN, d.unfamiliarDevice);
        assertNotNull(d.missingEvidence);
    }

    @Test
    void missingIdentityIsStillIncompleteForASmallPaymentWhileTheChangeIsRecent() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, null, T0, 0), payment(T0 + MIN, 100L, "USD"), cfg);
        assertEquals(Outcome.REVIEW_INCOMPLETE_CONTEXT, d.outcome);
    }

    @Test
    void identityIsNotRequiredWhenTheChangeIsOutsideTheWindow() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, null, T0, 0), payment(T0 + 30 * MIN, 7_500_000L, "USD"), cfg);
        assertEquals(Outcome.NO_RULE_MATCH, d.outcome);
        assertNull(d.missingEvidence);
    }

    @Test
    void missingBaselineIsIncomplete() {
        ReviewDecision d = PaymentReviewRule.evaluate(null, payment(T0, 7_500_000L, "USD"), cfg);
        assertEquals(Outcome.REVIEW_INCOMPLETE_CONTEXT, d.outcome);
        assertTrue(d.missingEvidence.contains("baseline"));
    }

    @Test
    void noChangeOnRecordIsNoMatch() {
        ReviewDecision d = PaymentReviewRule.evaluate(context(false, null, 0, 0), payment(T0, 7_500_000L, "USD"), cfg);
        assertEquals(Outcome.NO_RULE_MATCH, d.outcome);
        assertEquals(TriState.FALSE, d.recentChange);
    }

    @Test
    void unfamiliarDeviceOnAnotherChangeIsIgnored() {
        SupplierContext c = context(true, null, T0, 0);
        IdentityEvidence other = new IdentityEvidence();
        other.eventId = "evt-other";
        other.changeId = "CHG-OTHER";
        other.actorId = "actor-1";
        other.sessionId = "sess-1";
        other.deviceKnown = false;
        other.occurredAtMs = T0;
        c.matchedIdentities.add(other); // even if it somehow got here, it must not count
        ReviewDecision d = PaymentReviewRule.evaluate(c, payment(T0 + MIN, 7_500_000L, "USD"), cfg);
        assertEquals(TriState.UNKNOWN, d.unfamiliarDevice);
        assertEquals(Outcome.REVIEW_INCOMPLETE_CONTEXT, d.outcome);
    }

    @Test
    void configurableThreshold() {
        RuleConfig lower = new RuleConfig(1_000_000L, "USD", java.time.Duration.ofMinutes(10));
        ReviewDecision d = PaymentReviewRule.evaluate(context(true, false, T0, T0 + MIN), payment(T0 + 2 * MIN, 5_000_000L, "USD"), lower);
        assertEquals(Outcome.MANUAL_REVIEW, d.outcome);
    }
}
