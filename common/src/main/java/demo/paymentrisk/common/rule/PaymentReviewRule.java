package demo.paymentrisk.common.rule;

import demo.paymentrisk.common.BusinessEvent;
import demo.paymentrisk.common.Money;
import demo.paymentrisk.common.correlation.BankChange;
import demo.paymentrisk.common.correlation.IdentityEvidence;
import demo.paymentrisk.common.correlation.SupplierContext;

import java.util.ArrayList;
import java.util.List;

/**
 * The deterministic demo payment-review rule. Pure: no I/O, no clock, no Flink.
 *
 * <pre>
 * recent_change     = 0 &lt;= payment.occurred_at - bank_change.occurred_at &lt;= 10 minutes
 * matched_identity  = identity.change_id == bank_change.change_id
 *                     AND same actor and session as the change
 *                     AND identity.occurred_at &lt;= payment.occurred_at
 * unfamiliar_device = matched_identity AND device_is_known == false
 * large_payment     = currency == USD AND amount_minor &gt; 5_000_000
 *
 * MANUAL_REVIEW if recent_change AND unfamiliar_device AND large_payment
 * </pre>
 *
 * <p>Conditions are three-valued. Missing required evidence (no supplier baseline, or a recent
 * change with no linked identity activity) makes a condition UNKNOWN; if no condition is FALSE
 * the outcome is REVIEW_INCOMPLETE_CONTEXT, never an implicit pass. The rule is evaluated only
 * when the payment arrives and only against that payment's own business time.
 */
public final class PaymentReviewRule {
    private PaymentReviewRule() {}

    public static ReviewDecision evaluate(SupplierContext ctx, BusinessEvent payment, RuleConfig cfg) {
        ReviewDecision d = new ReviewDecision();
        List<String> missing = new ArrayList<>();
        long paymentAt = payment.occurredAtMs;

        // large_payment needs nothing but the payment itself.
        d.largePayment = TriState.of(cfg.currency.equals(payment.currency)
                && payment.amountMinor > cfg.largePaymentThresholdMinor);

        if (ctx == null || !ctx.baselineKnown) {
            missing.add("supplier baseline (SUPPLIER_REGISTERED) for " + payment.supplierId);
        }
        if (ctx != null) {
            d.contextVersionUsed = ctx.version;
            d.baselineEventId = ctx.baselineEventId;
        }

        BankChange change = ctx == null ? null : ctx.bankChange;
        if (ctx == null || !ctx.baselineKnown) {
            d.recentChange = TriState.UNKNOWN;
            d.unfamiliarDevice = TriState.UNKNOWN;
        } else if (change == null) {
            // Baseline known and no change on record: there is nothing recent and nothing to match.
            d.recentChange = TriState.FALSE;
            d.unfamiliarDevice = TriState.FALSE;
        } else {
            d.bankChangeEventId = change.eventId;
            long delta = paymentAt - change.occurredAtMs;
            d.secondsSinceBankChange = Math.floorDiv(delta, 1000L);
            d.recentChange = TriState.of(delta >= 0 && delta <= cfg.recentChangeWindowMs);

            IdentityEvidence unfamiliar = null;
            IdentityEvidence familiar = null;
            for (IdentityEvidence ev : ctx.matchedIdentities) {
                if (!ev.belongsTo(change) || ev.occurredAtMs > paymentAt) {
                    continue; // another modification, or activity after the payment was requested
                }
                if (!ev.deviceKnown && unfamiliar == null) {
                    unfamiliar = ev;
                } else if (ev.deviceKnown) {
                    familiar = ev;
                }
            }
            if (unfamiliar != null) {
                d.unfamiliarDevice = TriState.TRUE;
                d.identityEventId = unfamiliar.eventId;
            } else if (familiar != null) {
                d.unfamiliarDevice = TriState.FALSE;
                d.identityEventId = familiar.eventId;
            } else {
                d.unfamiliarDevice = TriState.UNKNOWN;
                // Identity evidence is required only while the change is inside the window.
                if (d.recentChange == TriState.TRUE) {
                    missing.add("identity activity linked to change " + change.changeId
                            + " (same actor and session, before the payment)");
                }
            }
        }

        // Required evidence: the supplier baseline always; linked identity activity when the
        // change is recent. Anything missing -> incomplete, regardless of the other conditions.
        if (!missing.isEmpty()) {
            d.outcome = Outcome.REVIEW_INCOMPLETE_CONTEXT;
        } else if (TriState.and(d.recentChange, d.unfamiliarDevice, d.largePayment) == TriState.TRUE) {
            d.outcome = Outcome.MANUAL_REVIEW;
        } else {
            d.outcome = Outcome.NO_RULE_MATCH;
        }
        d.missingEvidence = missing.isEmpty() ? null : String.join("; ", missing);
        d.explanation = explain(d, change, payment, cfg);
        return d;
    }

    /** A fixed template, filled with facts. This is deliberately not presented as AI output. */
    private static String explain(ReviewDecision d, BankChange change, BusinessEvent p, RuleConfig cfg) {
        String amount = Money.format(p.amountMinor, p.currency);
        StringBuilder s = new StringBuilder();
        switch (d.outcome) {
            case MANUAL_REVIEW:
                s.append("Route to manual review: ").append(amount)
                        .append(" requested ").append(d.secondsSinceBankChange).append("s after beneficiary change ")
                        .append(change.changeId).append(" (").append(change.beforeMasked).append(" -> ")
                        .append(change.afterMasked).append("), and that modification was made from a previously unseen device.");
                break;
            case NO_RULE_MATCH:
                s.append("No match for this demo rule: ");
                List<String> why = new ArrayList<>();
                if (d.recentChange == TriState.FALSE) {
                    why.add(change == null ? "no beneficiary change on record"
                            : "the beneficiary change is outside the " + (cfg.recentChangeWindowMs / 60000) + "-minute window ("
                            + d.secondsSinceBankChange + "s before the payment)");
                }
                if (d.unfamiliarDevice == TriState.FALSE && change != null) {
                    why.add("the linked modification used a known device");
                }
                if (d.largePayment == TriState.FALSE) {
                    why.add(amount + " is not above " + Money.format(cfg.largePaymentThresholdMinor, cfg.currency));
                }
                s.append(String.join("; ", why)).append('.');
                break;
            default:
                s.append("Cannot evaluate the rule: missing ").append(d.missingEvidence).append('.');
                break;
        }
        s.append(" This outcome does not authorize, execute or release any payment.");
        return s.toString();
    }
}
