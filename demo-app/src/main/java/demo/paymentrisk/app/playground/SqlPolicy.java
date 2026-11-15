package demo.paymentrisk.app.playground;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Keeps SQL Workspace statements scoped to this demo's predefined tables. The statement itself
 * is executed by Flink; this only decides whether it may be submitted.
 *
 * <ul>
 *   <li>One statement per execution.</li>
 *   <li>SELECT / WITH / VALUES, EXPLAIN, SHOW and DESCRIBE are allowed.</li>
 *   <li>INSERT only into {@code computed_results} (the staged ingestion input). Fluss tables are
 *       written only by the ingestion job.</li>
 *   <li>No DDL, SET, ADD JAR, LOAD MODULE, USE, STOP JOB or STATEMENT SET: the tables are
 *       predefined. Every leading keyword of a Flink 1.20 statement outside the supported set
 *       is refused; any other text goes to Flink, whose parser reports the error.</li>
 *   <li>{@code OPTIONS} hints may change only startup/bounded scan positions, never connection
 *       properties or topics.</li>
 * </ul>
 */
public final class SqlPolicy {
    public static final int MAX_LENGTH = 20_000;
    private static final Set<String> HINT_KEYS = Set.of(
            "scan.startup.mode", "scan.startup.specific-offsets", "scan.startup.timestamp-millis",
            "scan.bounded.mode", "scan.bounded.specific-offsets", "scan.bounded.timestamp-millis",
            "scan.startup.timestamp");
    private static final Set<String> INSERT_TARGETS = Set.of(
            "computed_results", "default_database.computed_results", "default_catalog.default_database.computed_results");
    /** Leading keywords of Flink 1.20 statements that are not available in the workspace. */
    private static final Set<String> REFUSED = Set.of(
            "CREATE", "DROP", "ALTER", "USE", "SET", "RESET", "ADD", "REMOVE", "LOAD", "UNLOAD", "EXECUTE",
            "BEGIN", "END", "DELETE", "UPDATE", "TRUNCATE", "CALL", "ANALYZE", "COMPILE", "STOP", "REPLACE",
            "MERGE", "GRANT", "REVOKE", "HELP", "QUIT", "EXIT", "SOURCE", "RUN", "OPTIMIZE");
    private static final Pattern HINT = Pattern.compile("/\\*\\+(.*?)\\*/", Pattern.DOTALL);
    private static final Pattern OPTION_KEY = Pattern.compile("'([^']*)'\\s*=");
    private static final Pattern INSERT = Pattern.compile("^INSERT\\s+(INTO|OVERWRITE)\\s+([`\\w.]+)", Pattern.CASE_INSENSITIVE);

    public static final class Rejected extends Exception {
        public Rejected(String message) {
            super(message);
        }
    }

    /** QUERY | INSERT | EXPLAIN | SHOW | DESCRIBE */
    public static String check(String statement) throws Rejected {
        if (statement == null || statement.isBlank()) {
            throw new Rejected("The statement is empty.");
        }
        if (statement.length() > MAX_LENGTH) {
            throw new Rejected("The statement is longer than " + MAX_LENGTH + " characters.");
        }
        String stripped = stripLiteralsAndComments(statement).trim();
        while (stripped.endsWith(";")) {
            stripped = stripped.substring(0, stripped.length() - 1).trim();
        }
        if (stripped.contains(";")) {
            throw new Rejected("Run one statement at a time (found more than one ';').");
        }
        String first = stripped.isEmpty() ? "" : stripped.split("\\s+")[0].toUpperCase(Locale.ROOT).replace("(", "");
        String kind;
        switch (first) {
            case "SELECT":
            case "WITH":
            case "VALUES":
                kind = "QUERY";
                break;
            case "INSERT":
                kind = "INSERT";
                break;
            case "EXPLAIN":
                kind = "EXPLAIN";
                break;
            case "SHOW":
                kind = "SHOW";
                break;
            case "DESCRIBE":
            case "DESC":
                kind = "DESCRIBE";
                break;
            default:
                if (first.isEmpty() || REFUSED.contains(first)) {
                    throw new Rejected((first.isEmpty() ? "This" : first) + " statements are not available here. Supported: "
                            + "SELECT, INSERT INTO computed_results, EXPLAIN, SHOW and DESCRIBE. The tables business_events, "
                            + "playground_events, computed_results and the Fluss catalog fluss_catalog are predefined.");
                }
                kind = "QUERY"; // not a statement keyword: Flink's parser reports the error
        }
        if ("INSERT".equals(kind)) {
            Matcher m = INSERT.matcher(stripped);
            if (!m.find()) {
                throw new Rejected("Could not read the INSERT target. Use INSERT INTO computed_results SELECT …");
            }
            if ("OVERWRITE".equalsIgnoreCase(m.group(1))) {
                throw new Rejected("INSERT OVERWRITE is not supported; use INSERT INTO computed_results.");
            }
            String target = m.group(2).replace("`", "").toLowerCase(Locale.ROOT);
            if (!INSERT_TARGETS.contains(target)) {
                throw new Rejected("INSERT is limited to computed_results, the input of the staged Fluss ingestion. "
                        + "Fluss tables (" + target + ") are written only by the ingestion job.");
            }
        }
        Matcher h = HINT.matcher(statement);
        while (h.find()) {
            String body = h.group(1);
            if (body.toUpperCase(Locale.ROOT).contains("OPTIONS")) {
                Matcher k = OPTION_KEY.matcher(body);
                while (k.find()) {
                    String key = k.group(1).trim();
                    if (!HINT_KEYS.contains(key)) {
                        throw new Rejected("OPTIONS hint '" + key + "' is not allowed. Hints may only set: " + String.join(", ", HINT_KEYS.stream().sorted().toList()) + ".");
                    }
                }
            }
        }
        return kind;
    }

    /** Removes string literals, quoted identifiers' contents are kept; -- and /* comments are removed. */
    static String stripLiteralsAndComments(String s) {
        StringBuilder b = new StringBuilder();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\'') {
                int j = i + 1;
                while (j < s.length()) {
                    if (s.charAt(j) == '\'' && j + 1 < s.length() && s.charAt(j + 1) == '\'') {
                        j += 2;
                    } else if (s.charAt(j) == '\'') {
                        break;
                    } else {
                        j++;
                    }
                }
                b.append("''");
                i = j + 1;
            } else if (c == '-' && i + 1 < s.length() && s.charAt(i + 1) == '-') {
                while (i < s.length() && s.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < s.length() && s.charAt(i + 1) == '*') {
                int end = s.indexOf("*/", i + 2);
                i = end < 0 ? s.length() : end + 2;
                b.append(' ');
            } else {
                b.append(c);
                i++;
            }
        }
        return b.toString();
    }

    private SqlPolicy() {}
}
