package demo.paymentrisk.app.playground;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlPolicyTest {

    @Test
    void classifiesSupportedStatements() throws Exception {
        assertEquals("QUERY", SqlPolicy.check("SELECT * FROM business_events;"));
        assertEquals("QUERY", SqlPolicy.check("-- comment\nWITH x AS (SELECT 1) SELECT * FROM x"));
        assertEquals("INSERT", SqlPolicy.check("INSERT INTO computed_results SELECT 'a','b','c','d','e','f'"));
        assertEquals("INSERT", SqlPolicy.check("insert into `default_catalog`.`default_database`.`computed_results` values ('a','b','c','d','e','f')"));
        assertEquals("SHOW", SqlPolicy.check("SHOW TABLES"));
        // A typo is not a statement keyword: it goes to Flink, whose parser explains the error.
        assertEquals("QUERY", SqlPolicy.check("SELEC 1"));
        assertEquals("QUERY", SqlPolicy.check("SELECT ';' AS semicolon_in_literal FROM business_events"));
        assertEquals("QUERY", SqlPolicy.check(
                "SELECT * FROM business_events /*+ OPTIONS('scan.startup.mode' = 'latest-offset', 'scan.bounded.mode' = 'unbounded') */"));
    }

    @Test
    void refusesStatementsOutsideTheDemoScope() {
        String ddl = assertThrows(SqlPolicy.Rejected.class,
                () -> SqlPolicy.check("CREATE TABLE t (a INT) WITH ('connector' = 'filesystem', 'path' = '/etc')")).getMessage();
        assertTrue(ddl.startsWith("CREATE statements are not available"), ddl);
        assertThrows(SqlPolicy.Rejected.class, () -> SqlPolicy.check("SET 'execution.runtime-mode' = 'batch'"));
        assertThrows(SqlPolicy.Rejected.class, () -> SqlPolicy.check("STOP JOB '0e4aa0ec6d468b3ace9fd0cc8f2ce268'"));
        assertThrows(SqlPolicy.Rejected.class, () -> SqlPolicy.check("EXECUTE STATEMENT SET BEGIN END"));
        assertThrows(SqlPolicy.Rejected.class, () -> SqlPolicy.check("ADD JAR '/tmp/x.jar'"));
        assertThrows(SqlPolicy.Rejected.class, () -> SqlPolicy.check("SELECT 1; SELECT 2"));
        String fluss = assertThrows(SqlPolicy.Rejected.class,
                () -> SqlPolicy.check("INSERT INTO fluss_catalog.payment_risk.payment_reviews SELECT * FROM x")).getMessage();
        assertTrue(fluss.contains("written only by the ingestion job"), fluss);
        String hint = assertThrows(SqlPolicy.Rejected.class, () -> SqlPolicy.check(
                "SELECT * FROM business_events /*+ OPTIONS('properties.bootstrap.servers' = 'evil:9092') */")).getMessage();
        assertTrue(hint.contains("properties.bootstrap.servers"), hint);
        assertThrows(SqlPolicy.Rejected.class, () -> SqlPolicy.check("   "));
    }
}
