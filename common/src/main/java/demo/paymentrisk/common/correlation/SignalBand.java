package demo.paymentrisk.common.correlation;

/**
 * Supplier signal band computed from the <em>latest observed</em> correlated signals.
 *
 * <p>It is contextual evidence, not a verdict: HIGH means suspicious evidence, not confirmed
 * fraud. The band is not actively expired with time; the payment rule separately checks the
 * age of the evidence against the payment's own business time.
 */
public enum SignalBand {
    NORMAL, ELEVATED, HIGH;

    public static SignalBand of(SupplierContext c) {
        if (c == null || c.bankChange == null) {
            return NORMAL;
        }
        return c.hasUnfamiliarDeviceEvidence() ? HIGH : ELEVATED;
    }

    public static String reason(SupplierContext c) {
        if (c == null || c.bankChange == null) {
            return "No beneficiary change observed; established details unchanged";
        }
        String change = "Beneficiary changed (" + c.bankChange.changeId + ")";
        if (c.hasUnfamiliarDeviceEvidence()) {
            return change + " in a modification made from a previously unseen device";
        }
        if (c.matchedIdentities.isEmpty()) {
            return change + "; no identity activity linked to this change yet";
        }
        return change + "; linked activity used a known device";
    }
}
