package demo.paymentrisk.common.correlation;

import demo.paymentrisk.common.BusinessEvent;

/** Identity-provider activity, kept either as matched evidence or in the bounded pre-change buffer. */
public class IdentityEvidence {
    public String eventId;
    public String changeId;
    public String actorId;
    public String sessionId;
    public String deviceId;
    public boolean deviceKnown;
    public String activity;
    public long occurredAtMs;
    // Kept so a buffered event's receipt can be re-written in full when it is finally applied.
    public Long emittedAtMs;
    public Integer kafkaPartition;
    public Long kafkaOffset;
    public String runId;
    public String supplierId;

    public IdentityEvidence() {}

    public static IdentityEvidence from(BusinessEvent e) {
        IdentityEvidence ev = new IdentityEvidence();
        ev.runId = e.runId;
        ev.eventId = e.eventId;
        ev.changeId = e.changeId;
        ev.actorId = e.actorId;
        ev.sessionId = e.sessionId;
        ev.deviceId = e.deviceId;
        ev.deviceKnown = Boolean.TRUE.equals(e.deviceKnown);
        ev.activity = e.activity;
        ev.occurredAtMs = e.occurredAtMs == null ? 0L : e.occurredAtMs;
        ev.emittedAtMs = e.emittedAtMs;
        ev.kafkaPartition = e.kafkaPartition;
        ev.kafkaOffset = e.kafkaOffset;
        ev.supplierId = e.supplierId;
        return ev;
    }

    /** Same modification: same change ID, made by the same actor in the same session. */
    public boolean belongsTo(BankChange change) {
        return change != null
                && change.changeId.equals(changeId)
                && change.actorId.equals(actorId)
                && change.sessionId.equals(sessionId);
    }
}
