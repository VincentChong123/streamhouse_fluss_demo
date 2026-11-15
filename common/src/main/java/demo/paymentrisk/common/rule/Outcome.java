package demo.paymentrisk.common.rule;

/** Policy outcome of the demo rule. None of these authorizes, executes or releases a payment. */
public enum Outcome {
    /** All three conditions hold: route to a human reviewer. */
    MANUAL_REVIEW("Manual review"),
    /** All required evidence present and at least one condition is false: "No match for this demo rule." */
    NO_RULE_MATCH("No match for this demo rule"),
    /** Required evidence is missing, so the rule cannot decide. Never an implicit pass. */
    REVIEW_INCOMPLETE_CONTEXT("Review: incomplete context");

    public final String label;

    Outcome(String label) {
        this.label = label;
    }
}
