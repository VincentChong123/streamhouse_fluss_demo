package demo.paymentrisk.common.rule;

/** Three-valued result of one rule condition. UNKNOWN means required evidence is missing. */
public enum TriState {
    TRUE, FALSE, UNKNOWN;

    public static TriState of(boolean b) {
        return b ? TRUE : FALSE;
    }

    /** Kleene AND: any FALSE wins, then any UNKNOWN, else TRUE. */
    public static TriState and(TriState... values) {
        boolean unknown = false;
        for (TriState v : values) {
            if (v == FALSE) {
                return FALSE;
            }
            if (v == UNKNOWN) {
                unknown = true;
            }
        }
        return unknown ? UNKNOWN : TRUE;
    }
}
