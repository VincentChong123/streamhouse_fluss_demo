package demo.paymentrisk.common.correlation;

import demo.paymentrisk.common.BusinessEvent;
import demo.paymentrisk.common.EventType;
import demo.paymentrisk.common.Money;
import demo.paymentrisk.common.OutputRow;
import demo.paymentrisk.common.TableSchemas;
import demo.paymentrisk.common.rule.PaymentReviewRule;
import demo.paymentrisk.common.rule.ReviewDecision;
import demo.paymentrisk.common.rule.RuleConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Stateful correlation for one (run, supplier) key, written as plain Java so it can be unit
 * tested. The Flink job calls {@link #process} from a {@code KeyedProcessFunction}.
 *
 * <ul>
 *   <li>Deduplicates by event ID <em>before</em> any version is incremented.</li>
 *   <li>Supplier baseline, bank change and each matched identity increment the context version.</li>
 *   <li>Identity activity that arrives before its bank change is held in a bounded buffer and
 *       applied when the change arrives.</li>
 *   <li>A payment is evaluated once per payment ID, against the context version current at that
 *       moment, and the review records which version it used.</li>
 * </ul>
 */
public final class CorrelationEngine implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Max identity events held per key while waiting for their bank change. */
    public static final int MAX_PENDING_IDENTITIES = 20;
    /** Max identity events kept as evidence for the current change. */
    public static final int MAX_MATCHED_IDENTITIES = 10;

    private final RuleConfig rule;

    public CorrelationEngine(RuleConfig rule) {
        this.rule = rule;
    }

    public RuleConfig rule() {
        return rule;
    }

    public List<OutputRow> process(BusinessEvent e, CorrelationState state, long nowMs) {
        List<OutputRow> out = new ArrayList<>();
        if (state.isEventSeen(e.eventId)) {
            return out; // duplicate delivery of the same event: no new output, no version change
        }
        state.markEventSeen(e.eventId, nowMs);

        if (e.invalidReason != null || e.type() == null) {
            SupplierContext ctx = state.loadContext();
            out.add(receipt(e, Disposition.REJECTED_INVALID, String.valueOf(e.invalidReason),
                    ctx == null ? null : ctx.version, nowMs));
            return out;
        }

        switch (e.type()) {
            case SUPPLIER_REGISTERED:
                onSupplierRegistered(e, state, nowMs, out);
                break;
            case BENEFICIARY_CHANGED:
                onBeneficiaryChanged(e, state, nowMs, out);
                break;
            case IDENTITY_ACTIVITY:
                onIdentityActivity(e, state, nowMs, out);
                break;
            case PAYMENT_REQUESTED:
                onPaymentRequested(e, state, nowMs, out);
                break;
            default:
                break;
        }
        return out;
    }

    private void onSupplierRegistered(BusinessEvent e, CorrelationState state, long now, List<OutputRow> out) {
        SupplierContext ctx = loadOrCreate(e, state);
        ctx.baselineKnown = true;
        ctx.baselineEventId = e.eventId;
        ctx.supplierName = e.supplierName;
        ctx.relationshipYears = e.relationshipYears;
        ctx.baselineBeneficiaryMasked = e.beneficiaryMasked;
        if (ctx.bankChange == null) {
            ctx.currentBeneficiaryMasked = e.beneficiaryMasked;
        }
        bump(ctx, e.eventId, e.eventType, e.occurredAtMs);
        state.storeContext(ctx);
        out.add(supplierRow(ctx, now));
        out.add(receipt(e, Disposition.APPLIED, "Supplier baseline recorded: " + e.supplierName + ", "
                + e.relationshipYears + " years, beneficiary " + e.beneficiaryMasked, ctx.version, now));
    }

    private void onBeneficiaryChanged(BusinessEvent e, CorrelationState state, long now, List<OutputRow> out) {
        SupplierContext ctx = loadOrCreate(e, state);
        BankChange c = new BankChange();
        c.changeId = e.changeId;
        c.eventId = e.eventId;
        c.actorId = e.actorId;
        c.sessionId = e.sessionId;
        c.beforeMasked = e.previousBeneficiaryMasked;
        c.afterMasked = e.newBeneficiaryMasked;
        c.occurredAtMs = e.occurredAtMs;
        ctx.bankChange = c;
        ctx.currentBeneficiaryMasked = e.newBeneficiaryMasked;
        ctx.matchedIdentities = new ArrayList<>(); // evidence belongs to the previous change
        bump(ctx, e.eventId, e.eventType, e.occurredAtMs);
        state.storeContext(ctx);
        out.add(supplierRow(ctx, now));
        out.add(receipt(e, Disposition.APPLIED, "Beneficiary change " + e.changeId + ": "
                + e.previousBeneficiaryMasked + " -> " + e.newBeneficiaryMasked + " by " + e.actorId, ctx.version, now));

        // Apply identity activity that arrived before this change, oldest first.
        Map<String, IdentityEvidence> pending = state.pendingIdentities();
        List<IdentityEvidence> waiting = new ArrayList<>();
        for (IdentityEvidence ev : pending.values()) {
            if (c.changeId.equals(ev.changeId)) {
                waiting.add(ev);
            }
        }
        waiting.sort(Comparator.comparing((IdentityEvidence ev) -> ev.kafkaOffset == null ? Long.MAX_VALUE : ev.kafkaOffset));
        for (IdentityEvidence ev : waiting) {
            state.removePendingIdentity(ev.eventId);
            if (ev.belongsTo(c)) {
                applyIdentity(ctx, ev, now);
                state.storeContext(ctx);
                out.add(supplierRow(ctx, now));
                out.add(receipt(ev, Disposition.APPLIED_FROM_BUFFER, identityDetail(ev, c)
                        + " (arrived before its bank change; applied once the change arrived)", ctx.version, now));
            } else {
                out.add(receipt(ev, Disposition.IGNORED_ACTOR_MISMATCH, "Change " + ev.changeId
                        + " was made by " + c.actorId + "/" + c.sessionId + ", not " + ev.actorId + "/" + ev.sessionId,
                        ctx.version, now));
            }
        }
    }

    private void onIdentityActivity(BusinessEvent e, CorrelationState state, long now, List<OutputRow> out) {
        SupplierContext ctx = state.loadContext();
        IdentityEvidence ev = IdentityEvidence.from(e);
        BankChange current = ctx == null ? null : ctx.bankChange;
        if (current != null && current.changeId.equals(ev.changeId)) {
            if (ev.belongsTo(current)) {
                applyIdentity(ctx, ev, now);
                state.storeContext(ctx);
                out.add(supplierRow(ctx, now));
                out.add(receipt(ev, Disposition.APPLIED, identityDetail(ev, current), ctx.version, now));
            } else {
                out.add(receipt(ev, Disposition.IGNORED_ACTOR_MISMATCH, "Change " + ev.changeId + " was made by "
                        + current.actorId + "/" + current.sessionId + ", not " + ev.actorId + "/" + ev.sessionId,
                        ctx.version, now));
            }
            return;
        }
        // No matching change yet: hold it (bounded) in case the change arrives later.
        Map<String, IdentityEvidence> pending = state.pendingIdentities();
        if (pending.size() >= MAX_PENDING_IDENTITIES) {
            pending.values().stream()
                    .min(Comparator.comparing((IdentityEvidence p) -> p.kafkaOffset == null ? Long.MIN_VALUE : p.kafkaOffset))
                    .ifPresent(oldest -> state.removePendingIdentity(oldest.eventId));
        }
        state.putPendingIdentity(ev);
        String why = current == null
                ? "No beneficiary change seen yet for this supplier"
                : "Linked to change " + ev.changeId + ", but the current change is " + current.changeId;
        out.add(receipt(ev, Disposition.BUFFERED_AWAITING_CHANGE, why + "; held (max " + MAX_PENDING_IDENTITIES
                + " per supplier) in case change " + ev.changeId + " arrives. Not used as evidence for any other change.",
                ctx == null ? null : ctx.version, now));
    }

    private void onPaymentRequested(BusinessEvent e, CorrelationState state, long now, List<OutputRow> out) {
        SupplierContext ctx = state.loadContext();
        String previous = state.reviewedPaymentEventId(e.paymentId);
        if (previous != null) {
            out.add(receipt(e, Disposition.DUPLICATE_PAYMENT_IGNORED, "Payment " + e.paymentId
                    + " was already reviewed from event " + previous + "; no second logical review",
                    ctx == null ? null : ctx.version, now));
            return;
        }
        ReviewDecision d = PaymentReviewRule.evaluate(ctx, e, rule);
        state.markPaymentReviewed(e.paymentId, e.eventId);
        out.add(reviewRow(e, d, now));
        out.add(receipt(e, Disposition.REVIEWED, d.outcome + " for " + Money.format(e.amountMinor, e.currency)
                + " (context v" + d.contextVersionUsed + ")", d.contextVersionUsed, now));
    }

    // ---------------------------------------------------------------------------------------

    private static SupplierContext loadOrCreate(BusinessEvent e, CorrelationState state) {
        SupplierContext ctx = state.loadContext();
        return ctx != null ? ctx : SupplierContext.empty(e.runId, e.supplierId);
    }

    private static void bump(SupplierContext ctx, String eventId, String eventType, Long occurredAt) {
        ctx.version++;
        ctx.lastSourceEventId = eventId;
        ctx.lastSourceEventType = eventType;
        ctx.lastSourceOccurredAtMs = occurredAt;
    }

    private static void applyIdentity(SupplierContext ctx, IdentityEvidence ev, long now) {
        if (ctx.matchedIdentities.size() >= MAX_MATCHED_IDENTITIES) {
            ctx.matchedIdentities.remove(0);
        }
        ctx.matchedIdentities.add(ev);
        bump(ctx, ev.eventId, EventType.IDENTITY_ACTIVITY.name(), ev.occurredAtMs);
    }

    private static String identityDetail(IdentityEvidence ev, BankChange c) {
        return "Identity activity linked to change " + c.changeId + " by " + ev.actorId + " on device " + ev.deviceId
                + (ev.deviceKnown ? " (known device)" : " (previously unseen device)");
    }

    private static OutputRow supplierRow(SupplierContext c, long now) {
        IdentityEvidence id = c.decisiveIdentity();
        BankChange b = c.bankChange;
        return OutputRow.of(TableSchemas.SUPPLIER_RISK)
                .set("run_id", c.runId)
                .set("supplier_id", c.supplierId)
                .set("supplier_name", c.supplierName)
                .set("relationship_years", c.relationshipYears)
                .set("baseline_beneficiary_masked", c.baselineBeneficiaryMasked)
                .set("current_beneficiary_masked", c.currentBeneficiaryMasked)
                .set("bank_change_id", b == null ? null : b.changeId)
                .set("bank_change_event_id", b == null ? null : b.eventId)
                .set("bank_change_occurred_at", b == null ? null : b.occurredAtMs)
                .set("bank_change_actor_id", b == null ? null : b.actorId)
                .set("bank_change_session_id", b == null ? null : b.sessionId)
                .set("identity_event_id", id == null ? null : id.eventId)
                .set("identity_device_id", id == null ? null : id.deviceId)
                .set("identity_device_known", id == null ? null : id.deviceKnown)
                .set("identity_occurred_at", id == null ? null : id.occurredAtMs)
                .set("unfamiliar_device_evidence", c.hasUnfamiliarDeviceEvidence())
                .set("signal_band", SignalBand.of(c).name())
                .set("signal_reason", SignalBand.reason(c))
                .set("context_version", c.version)
                .set("last_source_event_id", c.lastSourceEventId)
                .set("last_source_event_type", c.lastSourceEventType)
                .set("last_source_occurred_at", c.lastSourceOccurredAtMs)
                .set("updated_at", now);
    }

    private OutputRow reviewRow(BusinessEvent p, ReviewDecision d, long now) {
        return OutputRow.of(TableSchemas.PAYMENT_REVIEWS)
                .set("run_id", p.runId)
                .set("payment_id", p.paymentId)
                .set("supplier_id", p.supplierId)
                .set("amount_minor", p.amountMinor)
                .set("currency", p.currency)
                .set("payment_occurred_at", p.occurredAtMs)
                .set("outcome", d.outcome.name())
                .set("check_recent_change", d.recentChange.name())
                .set("check_unfamiliar_device", d.unfamiliarDevice.name())
                .set("check_large_payment", d.largePayment.name())
                .set("seconds_since_bank_change", d.secondsSinceBankChange)
                .set("explanation", d.explanation)
                .set("missing_evidence", d.missingEvidence)
                .set("context_version_used", d.contextVersionUsed)
                .set("evidence_bank_change_event_id", d.bankChangeEventId)
                .set("evidence_identity_event_id", d.identityEventId)
                .set("evidence_baseline_event_id", d.baselineEventId)
                .set("source_payment_event_id", p.eventId)
                .set("source_kafka_partition", p.kafkaPartition)
                .set("source_kafka_offset", p.kafkaOffset)
                .set("rule_version", RuleConfig.RULE_VERSION)
                .set("rule_parameters", rule.describe())
                .set("evaluated_at", now);
    }

    private static OutputRow receipt(BusinessEvent e, Disposition d, String detail, Integer version, long now) {
        return OutputRow.of(TableSchemas.EVENT_RECEIPTS)
                .set("run_id", e.runId)
                .set("event_id", e.eventId)
                .set("event_type", e.eventType)
                .set("supplier_id", e.supplierId)
                .set("disposition", d.name())
                .set("detail", detail)
                .set("context_version_after", version)
                .set("kafka_partition", e.kafkaPartition)
                .set("kafka_offset", e.kafkaOffset)
                .set("occurred_at", e.occurredAtMs)
                .set("emitted_at", e.emittedAtMs)
                .set("processed_at", now);
    }

    private static OutputRow receipt(IdentityEvidence ev, Disposition d, String detail, Integer version, long now) {
        BusinessEvent e = new BusinessEvent();
        e.runId = ev.runId;
        e.eventId = ev.eventId;
        e.eventType = EventType.IDENTITY_ACTIVITY.name();
        e.supplierId = ev.supplierId;
        e.kafkaPartition = ev.kafkaPartition;
        e.kafkaOffset = ev.kafkaOffset;
        e.occurredAtMs = ev.occurredAtMs;
        e.emittedAtMs = ev.emittedAtMs;
        return receipt(e, d, detail, version, now);
    }
}
