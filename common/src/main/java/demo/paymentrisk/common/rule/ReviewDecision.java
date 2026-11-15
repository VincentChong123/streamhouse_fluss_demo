package demo.paymentrisk.common.rule;

/** The rule's result plus the evidence references it used. */
public final class ReviewDecision {
    public Outcome outcome;
    public TriState recentChange;
    public TriState unfamiliarDevice;
    public TriState largePayment;
    public Long secondsSinceBankChange;
    public String explanation;
    public String missingEvidence;
    public Integer contextVersionUsed;
    public String bankChangeEventId;
    public String identityEventId;
    public String baselineEventId;

    @Override
    public String toString() {
        return outcome + " [recent_change=" + recentChange + ", unfamiliar_device=" + unfamiliarDevice
                + ", large_payment=" + largePayment + "] " + explanation;
    }
}
