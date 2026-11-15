package demo.paymentrisk.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComputedResultsTest {

    private static OutputRow supplierRow() {
        return OutputRow.of(TableSchemas.SUPPLIER_RISK)
                .set("run_id", "run-1").set("supplier_id", "SUP-1").set("context_version", 3)
                .set("unfamiliar_device_evidence", true).set("bank_change_occurred_at", 1_762_941_660_000L)
                .set("signal_band", "HIGH");
    }

    @Test
    void roundTripsAnEngineRow() {
        String json = ComputedResults.encode(supplierRow(), "proc-1", "flink-processing-job");
        ComputedResults.Parsed p = ComputedResults.parse(json);
        assertTrue(p.ok(), p.errors::toString);
        assertEquals("supplier_risk", p.targetTable);
        assertEquals(List.of("run-1", "SUP-1"), p.keyValues());
        assertEquals(3, p.values.get("context_version"));
        assertEquals(1_762_941_660_000L, p.values.get("bank_change_occurred_at"));
        Object[] row = p.rowValues("proc-1", "ingest-1");
        assertEquals("proc-1", row[TableSchemas.SUPPLIER_RISK.indexOf("computed_by")]);
        assertEquals("ingest-1", row[TableSchemas.SUPPLIER_RISK.indexOf("ingested_by")]);
        assertEquals("run-1|supplier_risk|SUP-1", ComputedResults.key(supplierRow()));
    }

    @Test
    void acceptsSqlStyleValues() {
        String json = "{\"contract\":\"computed-result/v1\",\"run_id\":\"run-1\",\"target_table\":\"payment_reviews\","
                + "\"execution_id\":\"sql-1\",\"producer\":\"flink-sql\",\"row_json\":"
                + "\"{\\\"run_id\\\":\\\"run-1\\\",\\\"payment_id\\\":\\\"PAY-1\\\",\\\"amount_minor\\\":7500000,"
                + "\\\"payment_occurred_at\\\":\\\"2026-11-12 10:03:00.000\\\",\\\"outcome\\\":\\\"MANUAL_REVIEW\\\"}\"}";
        ComputedResults.Parsed p = ComputedResults.parse(json);
        assertTrue(p.ok(), p.errors::toString);
        assertEquals(7_500_000L, p.values.get("amount_minor"));
        assertEquals(java.time.Instant.parse("2026-11-12T10:03:00Z").toEpochMilli(), p.values.get("payment_occurred_at"));
        assertNull(p.values.get("check_recent_change"));
    }

    @Test
    void explainsIncompatibleRecords() {
        String json = "{\"contract\":\"computed-result/v1\",\"run_id\":\"run-1\",\"target_table\":\"payment_reviews\","
                + "\"execution_id\":\"sql-1\",\"row_json\":{\"run_id\":\"run-2\",\"amount_minor\":\"7500000\",\"score\":1}}";
        ComputedResults.Parsed p = ComputedResults.parse(json);
        assertFalse(p.ok());
        String all = String.join(" | ", p.errors);
        assertTrue(all.contains("row_json.score is not a column of payment_reviews"), all);
        assertTrue(all.contains("amount_minor must be a JSON integer (BIGINT)"), all);
        assertTrue(all.contains("CAST(JSON_VALUE(...) AS BIGINT)"), all);
        assertTrue(all.contains("row_json.payment_id is required"), all);
        assertTrue(all.contains("must equal the envelope run_id"), all);

        assertFalse(ComputedResults.parse("not json").ok());
        assertTrue(ComputedResults.parse("{\"contract\":\"x\"}").errors.get(0).contains("contract must be"));
        assertTrue(String.join(" ", ComputedResults.parse("{\"contract\":\"computed-result/v1\",\"run_id\":\"r\","
                + "\"execution_id\":\"e\",\"target_table\":\"orders\",\"row_json\":\"{}\"}").errors).contains("target_table must be one of"));
    }
}
