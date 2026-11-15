package demo.paymentrisk.common.correlation;

import java.util.ArrayList;
import java.util.List;

/**
 * Correlated supplier context for one (run, supplier) key. Held in Flink keyed state and
 * mirrored, version by version, into the Fluss {@code supplier_risk} table.
 */
public class SupplierContext {
    public String runId;
    public String supplierId;

    public boolean baselineKnown;
    public String baselineEventId;
    public String supplierName;
    public Integer relationshipYears;
    public String baselineBeneficiaryMasked;
    public String currentBeneficiaryMasked;

    /** The latest beneficiary change (the rule always evaluates against this one). */
    public BankChange bankChange;
    /** Identity activity linked to {@link #bankChange} (same change, actor and session). Bounded. */
    public List<IdentityEvidence> matchedIdentities = new ArrayList<>();

    public int version;
    public String lastSourceEventId;
    public String lastSourceEventType;
    public Long lastSourceOccurredAtMs;

    public SupplierContext() {}

    public static SupplierContext empty(String runId, String supplierId) {
        SupplierContext c = new SupplierContext();
        c.runId = runId;
        c.supplierId = supplierId;
        return c;
    }

    /** First matched identity on an unfamiliar device, else the latest matched identity, else null. */
    public IdentityEvidence decisiveIdentity() {
        IdentityEvidence latest = null;
        for (IdentityEvidence ev : matchedIdentities) {
            if (!ev.deviceKnown) {
                return ev;
            }
            latest = ev;
        }
        return latest;
    }

    public boolean hasUnfamiliarDeviceEvidence() {
        for (IdentityEvidence ev : matchedIdentities) {
            if (!ev.deviceKnown) {
                return true;
            }
        }
        return false;
    }
}
