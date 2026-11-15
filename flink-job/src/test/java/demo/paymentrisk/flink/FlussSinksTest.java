package demo.paymentrisk.flink;

import demo.paymentrisk.common.TableSchemas;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlussSinksTest {
    @Test
    void insertConvertsTimestampColumnsAndListsEveryColumn() {
        String sql = FlussSinks.insertSql("payment_risk", TableSchemas.EVENT_RECEIPTS);
        assertTrue(sql.startsWith("INSERT INTO `fluss_catalog`.`payment_risk`.`event_receipts` (`run_id`, `event_id`"), sql);
        assertTrue(sql.contains("TO_TIMESTAMP_LTZ(`processed_at`, 3) AS `processed_at`"), sql);
        assertTrue(sql.endsWith("FROM `v_event_receipts`"), sql);
    }

    @Test
    void rowTypeMatchesTheTableContract() {
        RowTypeInfo t = FlussSinks.rowType(TableSchemas.SUPPLIER_RISK);
        assertEquals(TableSchemas.SUPPLIER_RISK.columns.size(), t.getArity());
        assertEquals("run_id", t.getFieldNames()[0]);
    }
}
