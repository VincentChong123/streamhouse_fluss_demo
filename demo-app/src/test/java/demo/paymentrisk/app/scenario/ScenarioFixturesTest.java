package demo.paymentrisk.app.scenario;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.common.BusinessEvent;
import demo.paymentrisk.common.EventCodec;
import demo.paymentrisk.common.Json;
import demo.paymentrisk.common.OutputRow;
import demo.paymentrisk.common.correlation.CorrelationEngine;
import demo.paymentrisk.common.correlation.InMemoryCorrelationState;
import demo.paymentrisk.common.rule.RuleConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UNIT level (no Kafka/Flink/Fluss): every fixture produces schema-valid events, and replaying
 * them through the same correlation engine the Flink job uses yields the fixture's expected
 * receipts and final rows. The end-to-end proof against real services is scripts/smoke.sh.
 */
class ScenarioFixturesTest {
    private static final Path DIR = Path.of("..", "scenarios");

    @Test
    void everyFixtureIsValidAndMeetsItsOwnExpectations() throws Exception {
        ScenarioCatalog catalog = new ScenarioCatalog(DIR);
        assertTrue(catalog.all().size() >= 8, "expected at least 8 fixtures");
        assertNotNull(catalog.get("northstar-suspicious"), "default scenario must exist");
        for (Scenario s : catalog.all()) {
            replay(s);
        }
    }

    private static void replay(Scenario s) {
        CorrelationEngine engine = new CorrelationEngine(RuleConfig.defaults());
        InMemoryCorrelationState state = new InMemoryCorrelationState();
        long anchor = Instant.parse("2026-11-12T10:00:00Z").toEpochMilli();
        Map<String, String> jsonByStep = new HashMap<>();
        Map<String, Object> lastSupplier = null;
        Map<String, Object> review = null;
        long offset = 0;
        for (Scenario.Step step : s.steps) {
            String json;
            if (step.duplicateOf != null) {
                json = jsonByStep.get(step.duplicateOf);
                assertNotNull(json, s.id + ": duplicate_of refers to an earlier step");
            } else {
                ObjectNode e = EventFactory.build("run-test", "corr-test", s.supplierId, step, EventFactory.newEventId(),
                        Instant.ofEpochMilli(anchor + step.businessOffsetSeconds * 1000L), Instant.ofEpochMilli(anchor));
                json = Json.write(e);
            }
            jsonByStep.put(step.id, json);
            BusinessEvent ev = EventCodec.parse(json);
            assertNotNull(ev, s.id + "/" + step.id + ": event must be identifiable");
            assertNull(ev.invalidReason, s.id + "/" + step.id + ": " + ev.invalidReason);
            ev.kafkaPartition = 0;
            ev.kafkaOffset = offset++;

            List<OutputRow> out = engine.process(ev, state, anchor);
            if (step.duplicateOf != null) {
                assertTrue(out.isEmpty(), s.id + "/" + step.id + ": a duplicate event_id must produce no output");
                continue;
            }
            OutputRow receipt = null;
            for (OutputRow r : out) {
                switch (r.table().name) {
                    case "supplier_risk": lastSupplier = r.asMap(); break;
                    case "payment_reviews": review = r.asMap(); break;
                    default:
                        if (ev.eventId.equals(r.get("event_id"))) {
                            receipt = r;
                        }
                }
            }
            assertNotNull(receipt, s.id + "/" + step.id + ": every non-duplicate event gets a receipt");
            JsonNode exp = step.expect;
            if (exp != null) {
                if (exp.has("disposition")) {
                    assertEquals(exp.get("disposition").asText(), receipt.get("disposition"), s.id + "/" + step.id);
                }
                if (exp.has("context_version")) {
                    assertEquals(exp.get("context_version").asInt(), receipt.get("context_version_after"), s.id + "/" + step.id);
                }
            }
        }
        JsonNode fin = s.expectedFinal;
        assertNotNull(fin, s.id + ": expected_final is required");
        assertMatches(s.id + " supplier", fin.path("supplier"), lastSupplier);
        if (fin.has("review")) {
            assertMatches(s.id + " review", fin.path("review"), review);
        }
    }

    private static void assertMatches(String what, JsonNode expected, Map<String, Object> row) {
        assertNotNull(row, what + ": row expected");
        for (Iterator<Map.Entry<String, JsonNode>> it = expected.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode actual = Json.mapper().valueToTree(row.get(e.getKey()));
            boolean eq = e.getValue().isNumber() && actual.isNumber() ? e.getValue().asLong() == actual.asLong()
                    : e.getValue().equals(actual);
            assertTrue(eq, what + "." + e.getKey() + ": expected " + e.getValue() + " but got " + actual);
        }
        assertFalse(expected.isEmpty(), what + ": empty expectation");
    }
}
