package demo.paymentrisk.common.correlation;

/** A recorded beneficiary change (from a simulated CDC-shaped event). */
public class BankChange {
    public String changeId;
    public String eventId;
    public String actorId;
    public String sessionId;
    public String beforeMasked;
    public String afterMasked;
    public long occurredAtMs;

    public BankChange() {}
}
