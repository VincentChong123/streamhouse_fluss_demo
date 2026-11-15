package demo.paymentrisk.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.common.TableSchemas.Column;
import demo.paymentrisk.common.TableSchemas.TableSpec;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The contract of the {@code computed-results} Kafka topic: the boundary between computation
 * (the processing job, or an INSERT from the SQL Workspace) and ingestion into Fluss.
 *
 * <pre>
 * {
 *   "contract":     "computed-result/v1",
 *   "run_id":       "run-…",
 *   "target_table": "supplier_risk" | "payment_reviews" | "event_receipts",
 *   "execution_id": "proc-…"            (who computed it),
 *   "producer":     "flink-processing-job" | "flink-sql" | …,
 *   "row_json":     "{…one row of the target table, as a JSON object in a string…}"
 * }
 * </pre>
 *
 * Inside {@code row_json}, every key must be a column of the target table. STRING columns take
 * JSON strings, INT/BIGINT take JSON integers, BOOLEAN takes true/false, and TIMESTAMP_LTZ(3)
 * takes an ISO-8601 instant ({@code 2026-11-12T10:00:00Z}), {@code yyyy-MM-dd HH:mm:ss[.SSS]}
 * (read as UTC) or epoch milliseconds. Primary-key columns are required. The provenance columns
 * {@code computed_by} and {@code ingested_by} are set by the ingestion job.
 *
 * <p>The demo app validates the records before submitting an ingestion job, and the ingestion
 * job validates them again with this same code, so incompatible records are reported, counted
 * and never written.
 */
public final class ComputedResults {
    public static final String CONTRACT = "computed-result/v1";

    private static final DateTimeFormatter SQL_TIMESTAMP = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart().appendLiteral(' ').optionalEnd()
            .optionalStart().appendLiteral('T').optionalEnd()
            .appendPattern("HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .toFormatter();

    private ComputedResults() {}

    // ------------------------------------------------------------------ encoding (processing job)

    /** Record key: {@code run_id|target_table|pk1|pk2}. */
    public static String key(OutputRow row) {
        StringBuilder b = new StringBuilder();
        b.append(row.get("run_id")).append('|').append(row.table().name);
        for (String pk : row.table().primaryKey) {
            if (!"run_id".equals(pk)) {
                b.append('|').append(row.get(pk));
            }
        }
        return b.toString();
    }

    /** One engine output row as a computed-result envelope (JSON text). */
    public static String encode(OutputRow row, String executionId, String producer) {
        ObjectNode r = Json.mapper().createObjectNode();
        for (Column c : row.table().columns) {
            if (TableSchemas.PROVENANCE_COLUMNS.contains(c.name)) {
                continue;
            }
            Object v = row.get(c.name);
            if (v == null) {
                r.putNull(c.name);
            } else if (c.type == TableSchemas.ColumnType.TIMESTAMP_LTZ_3) {
                r.put(c.name, Instant.ofEpochMilli((Long) v).toString());
            } else if (v instanceof String) {
                r.put(c.name, (String) v);
            } else if (v instanceof Integer) {
                r.put(c.name, (Integer) v);
            } else if (v instanceof Long) {
                r.put(c.name, (Long) v);
            } else if (v instanceof Boolean) {
                r.put(c.name, (Boolean) v);
            } else {
                r.put(c.name, String.valueOf(v));
            }
        }
        ObjectNode env = Json.mapper().createObjectNode();
        env.put("contract", CONTRACT);
        env.put("run_id", String.valueOf(row.get("run_id")));
        env.put("target_table", row.table().name);
        env.put("execution_id", executionId);
        env.put("producer", producer);
        env.put("row_json", Json.write(r));
        return Json.write(env);
    }

    // ------------------------------------------------------------------ parsing + validation

    /** A parsed record. {@link #errors} is empty when it satisfies the contract. */
    public static final class Parsed {
        public String contract;
        public String runId;
        public String targetTable;
        public String executionId;
        public String producer;
        public TableSpec spec;
        /** The row as sent (for display), or null if it could not be read. */
        public JsonNode row;
        /** Typed values in column order: String, Integer, Long, Boolean; timestamps as Long epoch ms. */
        public Map<String, Object> values = new LinkedHashMap<>();
        public final List<String> errors = new ArrayList<>();

        public boolean ok() {
            return errors.isEmpty();
        }

        /** Primary-key values in key order (null if invalid). */
        public List<String> keyValues() {
            if (spec == null || !ok()) {
                return null;
            }
            List<String> k = new ArrayList<>();
            for (String pk : spec.primaryKey) {
                k.add(String.valueOf(values.get(pk)));
            }
            return k;
        }

        /** Values in the table's column order, provenance columns included (for a Flink Row). */
        public Object[] rowValues(String computedBy, String ingestedBy) {
            Object[] out = new Object[spec.columns.size()];
            for (int i = 0; i < out.length; i++) {
                String n = spec.columns.get(i).name;
                out[i] = "computed_by".equals(n) ? computedBy : "ingested_by".equals(n) ? ingestedBy : values.get(n);
            }
            return out;
        }
    }

    public static Parsed parse(String value) {
        Parsed p = new Parsed();
        if (value == null || value.isBlank()) {
            p.errors.add("empty record value");
            return p;
        }
        JsonNode root;
        try {
            root = Json.mapper().readTree(value);
        } catch (Exception e) {
            p.errors.add("record value is not valid JSON: " + firstLine(e.getMessage()));
            return p;
        }
        if (root == null || !root.isObject()) {
            p.errors.add("record value must be a JSON object with contract, run_id, target_table, execution_id and row_json");
            return p;
        }
        p.contract = text(root, "contract");
        p.runId = text(root, "run_id");
        p.targetTable = text(root, "target_table");
        p.executionId = text(root, "execution_id");
        p.producer = text(root, "producer");
        if (!CONTRACT.equals(p.contract)) {
            p.errors.add("contract must be \"" + CONTRACT + "\" (found " + quote(p.contract) + ")");
        }
        if (blank(p.runId)) {
            p.errors.add("run_id is required");
        }
        if (blank(p.executionId)) {
            p.errors.add("execution_id is required (a label for whatever computed this record)");
        }
        p.spec = p.targetTable == null ? null : TableSchemas.byName(p.targetTable);
        if (p.spec == null) {
            p.errors.add("target_table must be one of supplier_risk, payment_reviews, event_receipts (found "
                    + quote(p.targetTable) + ")");
        }
        JsonNode rowNode = root.get("row_json");
        JsonNode row = null;
        if (rowNode != null && rowNode.isTextual()) {
            try {
                row = Json.mapper().readTree(rowNode.textValue());
            } catch (Exception e) {
                p.errors.add("row_json is not valid JSON: " + firstLine(e.getMessage()));
            }
        } else if (rowNode != null && rowNode.isObject()) {
            row = rowNode;
        }
        if (row == null || !row.isObject()) {
            if (p.errors.stream().noneMatch(x -> x.startsWith("row_json"))) {
                p.errors.add("row_json must hold one JSON object (the row for target_table)");
            }
            return p;
        }
        p.row = row;
        if (p.spec == null) {
            return p;
        }
        validateRow(p, row);
        return p;
    }

    private static void validateRow(Parsed p, JsonNode row) {
        TableSpec spec = p.spec;
        row.fieldNames().forEachRemaining(f -> {
            if (spec.indexOf(f) < 0) {
                p.errors.add("row_json." + f + " is not a column of " + spec.name);
            } else if (TableSchemas.PROVENANCE_COLUMNS.contains(f)) {
                p.errors.add("row_json." + f + " is set by the ingestion job; remove it");
            }
        });
        for (Column c : spec.columns) {
            if (TableSchemas.PROVENANCE_COLUMNS.contains(c.name)) {
                continue;
            }
            JsonNode v = row.get(c.name);
            Object typed = null;
            if (v != null && !v.isNull()) {
                typed = convert(c, v, p.errors);
            }
            p.values.put(c.name, typed);
        }
        for (String pk : spec.primaryKey) {
            Object v = p.values.get(pk);
            if (v == null || (v instanceof String && ((String) v).isBlank())) {
                p.errors.add("row_json." + pk + " is required (primary key of " + spec.name + " is " + spec.primaryKey + ")");
            }
        }
        Object rowRun = p.values.get("run_id");
        if (rowRun != null && p.runId != null && !p.runId.equals(rowRun)) {
            p.errors.add("row_json.run_id (" + rowRun + ") must equal the envelope run_id (" + p.runId + ")");
        }
    }

    private static Object convert(Column c, JsonNode v, List<String> errors) {
        String where = "row_json." + c.name;
        switch (c.type) {
            case STRING:
                if (v.isTextual()) {
                    return v.textValue();
                }
                errors.add(where + " must be a JSON string (STRING); found " + kind(v));
                return null;
            case INT:
                if (v.isIntegralNumber() && v.canConvertToInt()) {
                    return v.intValue();
                }
                errors.add(where + " must be a JSON integer (INT); found " + kind(v) + numericHint(v, "INT"));
                return null;
            case BIGINT:
                if (v.isIntegralNumber() && v.canConvertToLong()) {
                    return v.longValue();
                }
                errors.add(where + " must be a JSON integer (BIGINT); found " + kind(v) + numericHint(v, "BIGINT"));
                return null;
            case BOOLEAN:
                if (v.isBoolean()) {
                    return v.booleanValue();
                }
                errors.add(where + " must be true or false (BOOLEAN); found " + kind(v));
                return null;
            case TIMESTAMP_LTZ_3:
                if (v.isIntegralNumber() && v.canConvertToLong()) {
                    return v.longValue();
                }
                if (v.isTextual()) {
                    Long ms = parseTimestamp(v.textValue());
                    if (ms != null) {
                        return ms;
                    }
                }
                errors.add(where + " must be a timestamp (ISO-8601 instant such as 2026-11-12T10:00:00Z, "
                        + "'yyyy-MM-dd HH:mm:ss' in UTC, or epoch milliseconds); found " + kind(v));
                return null;
            default:
                errors.add(where + ": unsupported column type " + c.type);
                return null;
        }
    }

    static Long parseTimestamp(String s) {
        try {
            return Instant.parse(s).toEpochMilli();
        } catch (Exception ignored) {
            // fall through
        }
        try {
            return java.time.OffsetDateTime.parse(s).toInstant().toEpochMilli();
        } catch (Exception ignored) {
            // fall through
        }
        try {
            return LocalDateTime.parse(s.trim(), SQL_TIMESTAMP).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String numericHint(JsonNode v, String type) {
        return v.isTextual() ? " — in Flink SQL use CAST(JSON_VALUE(...) AS " + type + ")" : "";
    }

    private static String kind(JsonNode v) {
        if (v.isTextual()) {
            String t = v.textValue();
            return "string \"" + (t.length() > 40 ? t.substring(0, 40) + "…" : t) + "\"";
        }
        if (v.isNumber()) {
            return "number " + v.asText();
        }
        if (v.isBoolean()) {
            return "boolean " + v.asText();
        }
        return v.getNodeType().name().toLowerCase();
    }

    private static String text(JsonNode n, String f) {
        JsonNode v = n.get(f);
        return v == null || v.isNull() || !v.isTextual() ? null : v.textValue();
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private static String quote(String s) {
        return s == null ? "nothing" : "\"" + s + "\"";
    }

    private static String firstLine(String s) {
        if (s == null) {
            return "?";
        }
        int i = s.indexOf('\n');
        return i < 0 ? s : s.substring(0, i);
    }
}
