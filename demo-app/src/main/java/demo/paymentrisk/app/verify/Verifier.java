package demo.paymentrisk.app.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.app.AppConfig;
import demo.paymentrisk.app.infra.FlinkRest;
import demo.paymentrisk.app.infra.FlussGateway;
import demo.paymentrisk.app.infra.KafkaGateway;
import demo.paymentrisk.common.Json;
import demo.paymentrisk.common.TableSchemas;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Acceptance checks against the REAL running stack. Operations are started through the demo
 * app's HTTP API (the calls the browser makes); results are checked both in the app's state and
 * independently, with this process's own Kafka reads, Flink REST calls and native Fluss lookups.
 *
 * <p>Phases ({@code scripts/smoke.sh} stops and starts services between them):
 * {@code stages, playground, tm-down, tm-recovered, fluss-down, fluss-recovered, kafka-down,
 * kafka-recovered}.
 */
public final class Verifier {
    private static final Duration STAGE = Duration.ofMinutes(3);

    private final AppConfig cfg;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final FlussGateway fluss;
    private final FlinkRest flink;
    private final KafkaGateway kafka;
    private final List<String> failures = new ArrayList<>();
    private int passed;

    private Verifier(AppConfig cfg) {
        this.cfg = cfg;
        this.fluss = new FlussGateway(cfg.flussBootstrap, cfg.flussDatabase);
        this.flink = new FlinkRest(cfg.flinkRestUrl);
        this.kafka = new KafkaGateway(cfg.kafkaBootstrap);
    }

    public static int run(AppConfig cfg, String[] args) {
        String phase = "stages";
        for (int i = 0; i < args.length; i++) {
            if ("--phase".equals(args[i]) && i + 1 < args.length) {
                phase = args[++i];
            }
        }
        Verifier v = new Verifier(cfg);
        long t0 = System.currentTimeMillis();
        System.out.println("== verify phase: " + phase);
        try {
            switch (phase) {
                case "stages": v.stages(); break;
                case "playground": v.playground(); break;
                case "tm-down": v.tmDown(); break;
                case "tm-recovered": v.recovered("flink"); break;
                case "fluss-down": v.flussDown(); break;
                case "fluss-recovered": v.flussRecovered(); break;
                case "kafka-down": v.kafkaDown(); break;
                case "kafka-recovered": v.kafkaRecovered(); break;
                default:
                    System.out.println("unknown phase " + phase);
                    return 2;
            }
        } catch (Exception e) {
            v.fail("phase " + phase + " aborted", e.getClass().getSimpleName() + ": " + e.getMessage());
        } finally {
            v.fluss.close();
            v.kafka.close();
        }
        System.out.printf("== phase %s: %d passed, %d failed (%.0f s)%n", phase, v.passed, v.failures.size(),
                (System.currentTimeMillis() - t0) / 1000.0);
        return v.failures.isEmpty() ? 0 : 1;
    }

    // ================================================================== phase: the four stages

    private void stages() throws Exception {
        waitReady();
        int runsBefore = state().path("app").path("runs_created").asInt();
        JsonNode s0 = state();
        expect("no automatic work on startup", "NOT_STARTED", s0.path("run").path("stage1").path("status").asText(),
                "the startup run has not emitted anything; runs created so far: " + runsBefore);

        String runId = newRun();
        JsonNode run = state().path("run");
        String supplier = run.path("supplier_id").asText();
        String payment = run.path("payment_id").asText();
        check("stages 2-4 locked before stage 1", !run.path("stage2").path("can_run").asBoolean()
                && !run.path("stage3").path("can_run").asBoolean() && !run.path("stage4").path("can_run").asBoolean(),
                "blocked: " + run.path("stage2").path("blocked_reason").asText());
        expectHttp("POST stage 2 before stage 1 is refused", 409, post("/api/stages/2", "{}"));

        // ---- stage 1: Kafka
        expectHttp("POST stage 1", 202, post("/api/stages/1", "{}"));
        expectHttp("duplicate POST stage 1 while running is refused", 409, post("/api/stages/1", "{}"));
        JsonNode st1 = waitStage(runId, "stage1", STAGE).path("run");
        expect("stage 1 status", "SUCCEEDED", st1.path("stage1").path("status").asText(), st1.path("stage1").path("error").asText());
        List<String> eventIds = new ArrayList<>();
        long prev = -1;
        boolean contiguous = true;
        for (JsonNode e : st1.path("stage1").path("events")) {
            eventIds.add(e.path("event_id").asText());
            long off = e.path("offset").asLong(-1);
            contiguous &= "ACKNOWLEDGED".equals(e.path("status").asText()) && (prev < 0 || off == prev + 1);
            prev = off;
        }
        check("4 events acknowledged at consecutive offsets", eventIds.size() == 4 && contiguous, "event ids " + eventIds);
        long firstOffset = st1.path("stage1").path("events").path(0).path("offset").asLong();
        List<KafkaGateway.Rec> recs = kafka.read(cfg.inputTopic, 0, firstOffset, firstOffset + 4, 10, Duration.ofSeconds(15));
        Set<String> inKafka = new HashSet<>();
        for (KafkaGateway.Rec r : recs) {
            inKafka.add(Json.mapper().readTree(r.value).path("event_id").asText());
        }
        check("records are in Kafka at the acknowledged offsets (independent read)", inKafka.containsAll(eventIds), inKafka.toString());
        expectAbsent("after stage 1: no supplier_risk row in Fluss", TableSchemas.SUPPLIER_RISK, runId, supplier);
        expectAbsent("after stage 1: no payment_reviews row in Fluss", TableSchemas.PAYMENT_REVIEWS, runId, payment);

        // ---- refresh / reconnect does not repeat anything
        int historyBefore = state().path("history").size();
        for (int i = 0; i < 3; i++) {
            readFirstSseSnapshot();
            get("/");
        }
        JsonNode afterRefresh = state();
        check("refresh/SSE reconnect starts nothing", afterRefresh.path("history").size() == historyBefore
                && "NOT_STARTED".equals(afterRefresh.path("run").path("stage2").path("status").asText()),
                "history entries " + historyBefore + " -> " + afterRefresh.path("history").size());

        // ---- stage 2: Flink processing
        expectHttp("POST stage 2", 202, post("/api/stages/2", "{}"));
        JsonNode st2 = waitStage(runId, "stage2", STAGE).path("run");
        JsonNode p = st2.path("stage2");
        expect("stage 2 status", "SUCCEEDED", p.path("status").asText(), p.path("error").asText());
        JsonNode px = p.path("execution");
        String procJob = px.path("job_id").asText();
        expect("processing job FINISHED (Flink REST)", "FINISHED", flinkState(procJob), "job " + procJob);
        check("processing read this run's 4 events", px.path("input_records_this_run").asInt() == 4,
                "range [" + px.path("input_start").asText() + ", " + px.path("input_end").asText() + "), accumulators "
                        + px.path("accumulators"));
        JsonNode review = null;
        List<Integer> versions = new ArrayList<>();
        for (JsonNode r : px.path("results")) {
            if ("supplier_risk".equals(r.path("target_table").asText())) {
                versions.add(r.path("row").path("context_version").asInt());
            } else if ("payment_reviews".equals(r.path("target_table").asText())) {
                review = r.path("row");
            }
        }
        check("computed supplier context evolves v1 -> v2 -> v3", versions.equals(List.of(1, 2, 3)), "versions " + versions);
        checkReview("computed review (Kafka computed-results)", review, eventIds);
        expectAbsent("after stage 2: final supplier_risk table still unpopulated", TableSchemas.SUPPLIER_RISK, runId, supplier);
        expectAbsent("after stage 2: final payment_reviews table still unpopulated", TableSchemas.PAYMENT_REVIEWS, runId, payment);

        // ---- stage 3: Fluss ingestion
        expectHttp("POST stage 3", 202, post("/api/stages/3", "{}"));
        JsonNode st3 = waitStage(runId, "stage3", STAGE).path("run");
        JsonNode ix = st3.path("stage3").path("execution");
        expect("stage 3 status", "SUCCEEDED", st3.path("stage3").path("status").asText(), st3.path("stage3").path("error").asText());
        String ingestJob = ix.path("job_id").asText();
        expect("ingestion job FINISHED (Flink REST)", "FINISHED", flinkState(ingestJob), "job " + ingestJob);
        check("read-back confirmed every row", ix.path("read_back").path("verified").asBoolean()
                && ix.path("read_back").path("matched").asInt() >= 6, ix.path("read_back").path("matched").asText() + " rows");
        Map<String, Object> sup = lookup(TableSchemas.SUPPLIER_RISK, runId, supplier);
        Map<String, Object> rev = lookup(TableSchemas.PAYMENT_REVIEWS, runId, payment);
        check("Fluss supplier_risk (independent lookup)", sup != null && Integer.valueOf(3).equals(sup.get("context_version"))
                && "HIGH".equals(sup.get("signal_band")) && Boolean.TRUE.equals(sup.get("unfamiliar_device_evidence"))
                && "•••• 9077".equals(sup.get("current_beneficiary_masked")), String.valueOf(sup));
        check("Fluss provenance: computed_by processing, ingested_by ingestion",
                rev != null && px.path("execution_id").asText().equals(rev.get("computed_by"))
                        && ix.path("execution_id").asText().equals(rev.get("ingested_by")),
                rev == null ? "no row" : rev.get("computed_by") + " / " + rev.get("ingested_by"));

        // ---- stage 4: query Fluss
        expectHttp("POST stage 4", 202, post("/api/stages/4", "{}"));
        JsonNode st4 = waitStage(runId, "stage4", Duration.ofSeconds(30)).path("run");
        expect("stage 4 status", "SUCCEEDED", st4.path("stage4").path("status").asText(), st4.path("stage4").path("error").asText());
        JsonNode q = st4.path("stage4").path("result");
        checkReview("stage 4 review (native Fluss lookup)", q.path("review").path("row"), eventIds);
        check("stage 4 reports lookup timing and mechanism", q.path("review").path("duration_ms").asDouble() > 0
                && q.path("mechanism").asText().contains("lookup"), q.path("mechanism").asText());

        // ---- refresh after completion shows the same, repeats nothing
        int h2 = state().path("history").size();
        readFirstSseSnapshot();
        JsonNode again = state();
        check("refresh after completion repeats nothing", again.path("history").size() == h2
                && runId.equals(again.path("run").path("run_id").asText())
                && "SUCCEEDED".equals(again.path("run").path("stage4").path("status").asText()), "");

        // ---- a new run shows no stale results
        String run2 = newRun();
        JsonNode s2 = state().path("run");
        check("new run starts clean", !run2.equals(runId) && "NOT_STARTED".equals(s2.path("stage4").path("status").asText())
                && s2.path("stage4").path("result").isNull() && s2.path("stage2").path("execution").isNull(), run2);
        expectAbsent("new run: no Fluss rows for the new run ID", TableSchemas.SUPPLIER_RISK, run2, supplier);
        check("earlier run's rows remain in Fluss", lookup(TableSchemas.SUPPLIER_RISK, runId, supplier) != null, runId);
        System.out.println("VERIFIED_RUN_ID=" + runId);
    }

    private void checkReview(String name, JsonNode review, List<String> eventIds) {
        if (review == null || review.isMissingNode() || review.isNull()) {
            fail(name, "no payment review");
            return;
        }
        boolean ok = "MANUAL_REVIEW".equals(review.path("outcome").asText())
                && "TRUE".equals(review.path("check_recent_change").asText())
                && "TRUE".equals(review.path("check_unfamiliar_device").asText())
                && "TRUE".equals(review.path("check_large_payment").asText())
                && review.path("context_version_used").asInt() == 3
                && review.path("amount_minor").asLong() == 7_500_000L
                && eventIds.get(0).equals(review.path("evidence_baseline_event_id").asText())
                && eventIds.get(1).equals(review.path("evidence_bank_change_event_id").asText())
                && eventIds.get(2).equals(review.path("evidence_identity_event_id").asText())
                && eventIds.get(3).equals(review.path("source_payment_event_id").asText());
        check(name + ": MANUAL_REVIEW, 3 checks TRUE, context v3, evidence = emitted events", ok,
                review.path("outcome").asText() + " " + review.path("check_recent_change").asText() + "/"
                        + review.path("check_unfamiliar_device").asText() + "/" + review.path("check_large_payment").asText()
                        + " v" + review.path("context_version_used").asText());
    }

    // ================================================================== phase: playground

    private void playground() throws Exception {
        waitReady();
        String tag = "verify-" + UUID.randomUUID().toString().substring(0, 6);

        // ---- manual events
        HttpResponse<String> r = postEvents(cfg.playgroundTopic, "k1", "[{\"category\":\"" + tag + "-a\",\"amount\":100},"
                + "{\"category\":\"" + tag + "-a\",\"amount\":250},{\"category\":\"" + tag + "-b\",\"amount\":4000}]", true);
        JsonNode sent = Json.mapper().readTree(r.body()).path("records");
        check("manual batch acknowledged by Kafka", r.statusCode() == 200 && sent.size() == 3
                && sent.path(2).path("offset").asLong(-1) >= 0, r.body().length() > 300 ? r.body().substring(0, 300) : r.body());
        long off = sent.path(0).path("offset").asLong();
        List<KafkaGateway.Rec> back = kafka.read(cfg.playgroundTopic, 0, off, off + 1, 1, Duration.ofSeconds(10));
        check("manual record readable at its acknowledged offset", back.size() == 1 && back.get(0).value.contains(tag + "-a"), "");
        HttpResponse<String> bad = postEvents(cfg.playgroundTopic, "", "{\"category\": \"x\", }", false);
        check("invalid JSON is rejected with its position", bad.statusCode() == 400 && bad.body().contains("Invalid JSON at line"), bad.body());
        JsonNode tpl = templates("payment");
        ((ObjectNode) tpl.path("value").path("payload")).put("amount_minor", "lots");
        HttpResponse<String> badEvent = postEvents(cfg.inputTopic, tpl.path("key").asText(), Json.write(tpl.path("value")), false);
        check("business event violating the contract is rejected", badEvent.statusCode() == 400
                && badEvent.body().contains("amount_minor"), badEvent.body());

        // ---- two different SQL queries, different correct results
        JsonNode a = sql("SELECT JSON_VALUE(message, '$.category') AS category, COUNT(*) AS n, "
                + "SUM(CAST(JSON_VALUE(message, '$.amount') AS DOUBLE)) AS total FROM playground_events "
                + "WHERE JSON_VALUE(message, '$.category') LIKE '" + tag + "%' GROUP BY JSON_VALUE(message, '$.category')", "batch", "sql");
        check("SQL A (aggregate): 2 groups, a=2/350, b=1/4000", "FINISHED".equals(a.path("status").asText())
                && rowsAsText(a).contains("[\"" + tag + "-a\",2,350.0]") && rowsAsText(a).contains("[\"" + tag + "-b\",1,4000.0]"),
                a.path("status").asText() + " " + rowsAsText(a) + " " + a.path("error").asText());
        JsonNode b = sql("SELECT JSON_VALUE(message, '$.category') AS category, CAST(JSON_VALUE(message, '$.amount') AS DOUBLE) AS amount "
                + "FROM playground_events WHERE JSON_VALUE(message, '$.category') LIKE '" + tag + "%' "
                + "AND CAST(JSON_VALUE(message, '$.amount') AS DOUBLE) >= 1000", "batch", "sql");
        check("SQL B (filter): only the 4000 record", "FINISHED".equals(b.path("status").asText())
                && b.path("rows").size() == 1 && rowsAsText(b).contains("4000.0"), rowsAsText(b));
        check("SQL executions ran as Flink jobs", !a.path("job_id").asText().isEmpty() && "FINISHED".equals(flinkState(a.path("job_id").asText())),
                a.path("job_id").asText());
        JsonNode invalid = sql("SELEC 1", "batch", "sql");
        check("invalid SQL reports Flink's parse error", "FAILED".equals(invalid.path("status").asText())
                && invalid.path("error").asText().toLowerCase().contains("parse"), invalid.path("error").asText());
        HttpResponse<String> ddl = post("/api/sql/executions", Json.write(Json.mapper().createObjectNode()
                .put("statement", "CREATE TABLE x (a INT) WITH ('connector' = 'filesystem')").put("mode", "batch")));
        check("DDL outside the demo's tables is refused", ddl.statusCode() == 400, ddl.body());

        // ---- cancellation of a streaming query
        HttpResponse<String> sr = post("/api/sql/executions", Json.write(Json.mapper().createObjectNode()
                .put("statement", "SELECT kafka_offset, event_type FROM business_events /*+ OPTIONS('scan.startup.mode' = 'latest-offset', "
                        + "'scan.bounded.mode' = 'unbounded') */").put("mode", "streaming").put("workspace", "sql")));
        String sid = Json.mapper().readTree(sr.body()).path("execution_id").asText();
        JsonNode running = waitFor("streaming query running with a job", Duration.ofSeconds(90), st -> {
            JsonNode x = sqlExec(st, sid);
            return x != null && "RUNNING".equals(x.path("status").asText()) && "RUNNING".equals(flinkState(x.path("job_id").asText(null)));
        });
        String streamJob = sqlExec(running, sid).path("job_id").asText();
        expectHttp("cancel streaming query", 202, post("/api/sql/executions/" + sid + "/cancel", "{}"));
        JsonNode canceled = waitFor("streaming query cancelled", Duration.ofSeconds(60), st -> {
            JsonNode x = sqlExec(st, sid);
            return x != null && !"RUNNING".equals(x.path("status").asText());
        });
        check("cancellation stops the Flink job", "CANCELED".equals(sqlExec(canceled, sid).path("status").asText())
                && "CANCELED".equals(flinkState(streamJob)), "job " + streamJob + " " + flinkState(streamJob));

        // ---- offset ranges, SQL INSERT consumed by ingestion, SQL and Python on Fluss
        String runId = newRun();
        JsonNode run = state().path("run");
        String supplier = run.path("supplier_id").asText();
        String payment = run.path("payment_id").asText();
        post("/api/stages/1", "{}");
        waitStage(runId, "stage1", STAGE);
        post("/api/stages/2", "{}");
        JsonNode e1 = waitStage(runId, "stage2", STAGE).path("run").path("stage2").path("execution");
        JsonNode late = templates("payment");
        String lateEventId = late.path("value").path("event_id").asText();
        String latePayment = late.path("value").path("payload").path("payment_id").asText();
        HttpResponse<String> lr = postEvents(cfg.inputTopic, late.path("key").asText(), Json.write(late.path("value")), false);
        long lateOffset = Json.mapper().readTree(lr.body()).path("records").path(0).path("offset").asLong(-1);
        check("late event acknowledged after the first processing range", lr.statusCode() == 200
                && lateOffset >= e1.path("input_end").asLong(), "offset " + lateOffset + ", range end " + e1.path("input_end").asText());
        check("first processing execution excluded the late event", !Json.write(e1.path("results")).contains(lateEventId), lateEventId);
        post("/api/stages/2", "{}");
        JsonNode e2 = waitStage(runId, "stage2", STAGE).path("run").path("stage2").path("execution");
        check("second processing execution included the late event", e2.path("input_records_this_run").asInt() == 5
                && Json.write(e2.path("results")).contains(lateEventId), "records " + e2.path("input_records_this_run").asText());

        JsonNode ins = sql(example("insert-amount-review"), "batch", "sql");
        check("SQL INSERT wrote valid computed results", "FINISHED".equals(ins.path("status").asText())
                && ins.path("output").path("valid").asInt() == 2, Json.write(ins.path("output")) + " " + ins.path("error").asText());
        JsonNode insBad = sql("INSERT INTO computed_results VALUES ('computed-result/v1', '" + runId + "', 'payment_reviews', "
                + "'sql-bad', 'flink-sql', '{\"run_id\":\"" + runId + "\",\"payment_id\":\"PAY-BAD\",\"amount_minor\":\"oops\"}')", "batch", "sql");
        check("SQL INSERT with an incompatible row is flagged", "FINISHED".equals(insBad.path("status").asText())
                && insBad.path("output").path("invalid").asInt() == 1, Json.write(insBad.path("output")));
        post("/api/stages/3", "{}");
        JsonNode st3 = waitStage(runId, "stage3", STAGE).path("run").path("stage3");
        expect("ingestion after SQL INSERT", "SUCCEEDED", st3.path("status").asText(), st3.path("error").asText());
        check("ingestion reports the incompatible record and why", st3.path("execution").path("rejected").size() == 1
                && Json.write(st3.path("execution").path("rejected")).contains("amount_minor must be a JSON integer"),
                Json.write(st3.path("execution").path("rejected")));
        Map<String, Object> sqlReview = lookup(TableSchemas.PAYMENT_REVIEWS, runId, payment);
        check("Fluss holds the SQL-computed review (computed_by sql-amount-review)", sqlReview != null
                && "sql-amount-review".equals(sqlReview.get("computed_by")) && "sql-amount-review/v1".equals(sqlReview.get("rule_version")),
                String.valueOf(sqlReview));
        check("late payment ingested too", lookup(TableSchemas.PAYMENT_REVIEWS, runId, latePayment) != null, latePayment);
        check("SQL-rejected record was not written", lookup(TableSchemas.PAYMENT_REVIEWS, runId, "PAY-BAD") == null, "PAY-BAD");

        JsonNode fq = sql(example("supplier-point"), "batch", "query");
        check("Fluss via Flink SQL returns this run's supplier row", "FINISHED".equals(fq.path("status").asText())
                && rowsAsText(fq).contains(runId) && rowsAsText(fq).contains("HIGH"), rowsAsText(fq) + " " + fq.path("error").asText());
        JsonNode agg = sql(example("reviews-by-outcome"), "batch", "query");
        check("Fluss full-table aggregate via Flink SQL", "FINISHED".equals(agg.path("status").asText())
                && rowsAsText(agg).contains("MANUAL_REVIEW"), rowsAsText(agg) + " " + agg.path("error").asText());

        JsonNode py = python(Json.mapper().readTree(get("/api/python/example").body()).path("code").asText());
        check("Python (pyfluss) returns this run's Fluss rows", "FINISHED".equals(py.path("status").asText())
                && py.path("stdout").asText().contains(runId) && py.path("stdout").asText().contains("\"context_version\": 3"),
                py.path("status").asText() + " " + py.path("error").asText());
        JsonNode pyBad = python("print('x'\n");
        check("invalid Python returns the interpreter error", "FAILED".equals(pyBad.path("status").asText())
                && pyBad.path("stderr").asText().contains("SyntaxError"), pyBad.path("error").asText());
        HttpResponse<String> pr = post("/api/python/executions", Json.write(Json.mapper().createObjectNode()
                .put("code", "import time\nprint('started', flush=True)\ntime.sleep(60)\n")));
        String pid = Json.mapper().readTree(pr.body()).path("execution_id").asText();
        sleep(2000);
        expectHttp("cancel Python execution", 202, post("/api/python/executions/" + pid + "/cancel", "{}"));
        JsonNode pyc = waitFor("python cancelled", Duration.ofSeconds(30), st -> {
            JsonNode x = find(st.path("python_executions"), pid);
            return x != null && !"RUNNING".equals(x.path("status").asText());
        });
        expect("Python cancellation", "CANCELED", find(pyc.path("python_executions"), pid).path("status").asText(), "");
        expectAbsent("supplier rows exist only for runs that were ingested", TableSchemas.SUPPLIER_RISK, "run-never-created", supplier);
    }

    // ================================================================== failure phases

    private void tmDown() throws Exception {
        waitFor("Flink reports no TaskManager", Duration.ofMinutes(2), s -> "DOWN".equals(component(s, "flink")));
        pass("health reflects TaskManager loss", componentDetail(state(), "flink"));
        String runId = newRun();
        post("/api/stages/1", "{}");
        JsonNode s1 = waitStage(runId, "stage1", STAGE).path("run");
        expect("stage 1 still works (Kafka is up)", "SUCCEEDED", s1.path("stage1").path("status").asText(), "");
        post("/api/stages/2", "{}");
        JsonNode s2 = waitStage(runId, "stage2", STAGE).path("run").path("stage2");
        check("stage 2 fails with a TaskManager message", "FAILED".equals(s2.path("status").asText())
                && s2.path("error").asText().contains("TaskManager"), s2.path("error").asText());
        check("stage 3 stays locked", !s2.isMissingNode() && !state().path("run").path("stage3").path("can_run").asBoolean(), "");
        expectAbsent("no Fluss rows were written for this run", TableSchemas.SUPPLIER_RISK, runId, s1.path("supplier_id").asText());
    }

    private void recovered(String component) throws Exception {
        waitReady();
        pass("health recovered", componentDetail(state(), component));
        String runId = newRun();
        for (int i = 1; i <= 4; i++) {
            post("/api/stages/" + i, "{}");
            JsonNode s = waitStage(runId, "stage" + i, STAGE).path("run").path("stage" + i);
            expect("after recovery: stage " + i, "SUCCEEDED", s.path("status").asText(), s.path("error").asText());
        }
    }

    private void flussDown() throws Exception {
        waitFor("Fluss reported DOWN", Duration.ofMinutes(2), s -> "DOWN".equals(component(s, "fluss")));
        pass("health reflects TabletServer loss", componentDetail(state(), "fluss"));
        String runId = newRun();
        post("/api/stages/1", "{}");
        waitStage(runId, "stage1", STAGE);
        post("/api/stages/2", "{}");
        JsonNode s2 = waitStage(runId, "stage2", STAGE).path("run").path("stage2");
        expect("stage 2 works without Fluss (it writes to Kafka)", "SUCCEEDED", s2.path("status").asText(), s2.path("error").asText());
        post("/api/stages/3", "{}");
        JsonNode s3 = waitStage(runId, "stage3", STAGE).path("run").path("stage3");
        check("stage 3 fails with a Fluss message", "FAILED".equals(s3.path("status").asText())
                && s3.path("error").asText().contains("Fluss"), s3.path("error").asText());
        expectHttp("stage 4 refused (no confirmed persistence)", 409, post("/api/stages/4", "{}"));
        HttpResponse<String> lk = get("/api/lookup/supplier_risk?run_id=" + runId + "&supplier_id=SUP-NORTHSTAR");
        expect("direct lookup reports the failure", "503", String.valueOf(lk.statusCode()), lk.body().length() > 200 ? lk.body().substring(0, 200) : lk.body());
        System.out.println("PENDING_RUN_ID=" + runId);
    }

    private void flussRecovered() throws Exception {
        waitReady();
        JsonNode run = state().path("run");
        String runId = run.path("run_id").asText();
        check("the failed run is still current", "FAILED".equals(run.path("stage3").path("status").asText()), runId);
        post("/api/stages/3", "{}");
        JsonNode s3 = waitStage(runId, "stage3", STAGE).path("run").path("stage3");
        expect("stage 3 retry after Fluss recovery", "SUCCEEDED", s3.path("status").asText(), s3.path("error").asText());
        post("/api/stages/4", "{}");
        JsonNode s4 = waitStage(runId, "stage4", Duration.ofSeconds(30)).path("run").path("stage4");
        expect("stage 4 after recovery", "SUCCEEDED", s4.path("status").asText(), s4.path("error").asText());
        expect("outcome after recovery", "MANUAL_REVIEW", s4.path("result").path("review").path("row").path("outcome").asText(), "");
    }

    private void kafkaDown() throws Exception {
        waitFor("Kafka reported DOWN", Duration.ofMinutes(2), s -> "DOWN".equals(component(s, "kafka")));
        String runId = newRun();
        post("/api/stages/1", "{}");
        JsonNode s1 = waitStage(runId, "stage1", Duration.ofMinutes(2)).path("run").path("stage1");
        check("stage 1 fails when Kafka is down", "FAILED".equals(s1.path("status").asText())
                && !s1.path("error").asText().isEmpty(), s1.path("error").asText());
        check("stage 2 stays locked", !state().path("run").path("stage2").path("can_run").asBoolean(), "");
    }

    private void kafkaRecovered() throws Exception {
        waitReady();
        JsonNode run = state().path("run");
        String runId = run.path("run_id").asText();
        post("/api/stages/1", "{}");
        JsonNode s1 = waitStage(runId, "stage1", STAGE).path("run").path("stage1");
        expect("stage 1 retry after Kafka recovery", "SUCCEEDED", s1.path("status").asText(), s1.path("error").asText());
    }

    // ================================================================== helpers

    private void waitReady() throws Exception {
        JsonNode s = waitFor("Kafka, Flink and Fluss UP", Duration.ofMinutes(6),
                st -> "READY".equals(st.path("health").path("overall").asText()));
        pass("services ready", componentSummary(s));
    }

    private String newRun() throws Exception {
        HttpResponse<String> r = post("/api/runs", "{}");
        if (r.statusCode() != 201) {
            throw new IllegalStateException("POST /api/runs -> " + r.statusCode() + " " + r.body());
        }
        return Json.mapper().readTree(r.body()).path("run_id").asText();
    }

    private JsonNode waitStage(String runId, String stage, Duration timeout) throws Exception {
        return waitFor(runId + " " + stage + " to finish", timeout, s -> runId.equals(s.path("run").path("run_id").asText())
                && !"RUNNING".equals(s.path("run").path(stage).path("status").asText())
                && s.path("app").path("busy_operation").isNull());
    }

    private JsonNode templates(String id) throws Exception {
        for (JsonNode t : Json.mapper().readTree(get("/api/events/templates").body())) {
            if (id.equals(t.path("id").asText())) {
                return t;
            }
        }
        throw new IllegalStateException("template " + id + " not found");
    }

    private String example(String id) throws Exception {
        for (JsonNode e : Json.mapper().readTree(get("/api/sql/examples").body())) {
            if (id.equals(e.path("id").asText())) {
                return e.path("statement").asText();
            }
        }
        throw new IllegalStateException("example " + id + " not found");
    }

    private HttpResponse<String> postEvents(String topic, String key, String payload, boolean batch) throws Exception {
        return post("/api/events", Json.write(Json.mapper().createObjectNode().put("topic", topic).put("key", key)
                .put("payload", payload).put("batch", batch)));
    }

    private JsonNode sql(String statement, String mode, String workspace) throws Exception {
        HttpResponse<String> r = post("/api/sql/executions", Json.write(Json.mapper().createObjectNode()
                .put("statement", statement).put("mode", mode).put("workspace", workspace)));
        if (r.statusCode() != 202) {
            ObjectNode o = Json.mapper().createObjectNode().put("status", "REJECTED_" + r.statusCode());
            o.put("error", Json.mapper().readTree(r.body()).path("error").asText());
            return o;
        }
        String id = Json.mapper().readTree(r.body()).path("execution_id").asText();
        JsonNode s = waitFor("SQL " + id, Duration.ofMinutes(3), st -> {
            JsonNode x = sqlExec(st, id);
            return x != null && Set.of("FINISHED", "FAILED", "CANCELED").contains(x.path("status").asText());
        });
        return sqlExec(s, id);
    }

    private JsonNode python(String code) throws Exception {
        HttpResponse<String> r = post("/api/python/executions", Json.write(Json.mapper().createObjectNode().put("code", code)));
        if (r.statusCode() != 202) {
            return Json.mapper().createObjectNode().put("status", "REJECTED_" + r.statusCode()).put("error", r.body());
        }
        String id = Json.mapper().readTree(r.body()).path("execution_id").asText();
        JsonNode s = waitFor("python " + id, Duration.ofMinutes(2), st -> {
            JsonNode x = find(st.path("python_executions"), id);
            return x != null && !"RUNNING".equals(x.path("status").asText());
        });
        return find(s.path("python_executions"), id);
    }

    private static JsonNode sqlExec(JsonNode state, String id) {
        return find(state.path("sql_executions"), id);
    }

    private static JsonNode find(JsonNode arr, String id) {
        for (JsonNode x : arr) {
            if (id.equals(x.path("id").asText())) {
                return x;
            }
        }
        return null;
    }

    private static String rowsAsText(JsonNode x) {
        StringBuilder b = new StringBuilder();
        for (JsonNode r : x.path("rows")) {
            b.append(Json.write(r.path("fields"))).append(' ');
        }
        return b.toString().trim();
    }

    private String flinkState(String jobId) {
        if (jobId == null || jobId.isEmpty()) {
            return "NO_JOB";
        }
        try {
            return flink.job(jobId).path("state").asText();
        } catch (Exception e) {
            return "UNKNOWN (" + e.getMessage() + ")";
        }
    }

    private Map<String, Object> lookup(TableSchemas.TableSpec spec, String k1, String k2) {
        FlussGateway.Observation o = fluss.lookup(spec, List.of(k1, k2), Duration.ofSeconds(5));
        if (!o.ok) {
            throw new IllegalStateException("lookup " + spec.name + " failed: " + o.error);
        }
        return o.found ? o.row : null;
    }

    private void expectAbsent(String name, TableSchemas.TableSpec spec, String k1, String k2) {
        try {
            Map<String, Object> row = lookup(spec, k1, k2);
            check(name, row == null, row == null ? "lookup found no row" : "unexpected row " + row);
        } catch (Exception e) {
            fail(name, e.getMessage());
        }
    }

    private JsonNode state() throws Exception {
        return Json.mapper().readTree(get("/api/state").body());
    }

    private JsonNode waitFor(String what, Duration timeout, Predicate<JsonNode> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        JsonNode last = null;
        Exception lastError = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = state();
                if (condition.test(last)) {
                    return last;
                }
            } catch (Exception e) {
                lastError = e;
            }
            sleep(1000);
        }
        throw new IllegalStateException("timed out after " + timeout.toSeconds() + " s waiting for: " + what
                + (last == null ? (lastError == null ? "" : " (" + lastError.getMessage() + ")") : " (health " + componentSummary(last) + ")"));
    }

    private JsonNode readFirstSseSnapshot() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(cfg.demoAppUrl + "/api/stream")).timeout(Duration.ofSeconds(10)).GET().build();
        HttpResponse<Stream<String>> r = http.send(req, HttpResponse.BodyHandlers.ofLines());
        try (Stream<String> lines = r.body()) {
            String data = lines.filter(l -> l.startsWith("data: ")).findFirst().orElseThrow();
            return Json.mapper().readTree(data.substring(6));
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(cfg.demoAppUrl + path)).timeout(Duration.ofSeconds(15)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String json) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(cfg.demoAppUrl + path)).timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static String component(JsonNode s, String id) {
        for (JsonNode c : s.path("health").path("components")) {
            if (id.equals(c.path("id").asText())) {
                return c.path("status").asText();
            }
        }
        return "?";
    }

    private static String componentDetail(JsonNode s, String id) {
        for (JsonNode c : s.path("health").path("components")) {
            if (id.equals(c.path("id").asText())) {
                return c.path("detail").asText();
            }
        }
        return "?";
    }

    private static String componentSummary(JsonNode s) {
        StringBuilder b = new StringBuilder();
        for (JsonNode c : s.path("health").path("components")) {
            b.append(c.path("id").asText()).append('=').append(c.path("status").asText()).append(' ');
        }
        return b.toString().trim();
    }

    private void expectHttp(String name, int expected, HttpResponse<String> r) {
        check(name, r.statusCode() == expected, "HTTP " + r.statusCode() + (r.statusCode() == expected ? "" : " " + r.body()));
    }

    private void expect(String name, String expected, String actual, String detail) {
        check(name, expected.equals(actual), (expected.equals(actual) ? actual : "expected " + expected + " but was " + actual)
                + (detail == null || detail.isBlank() || "null".equals(detail) ? "" : " — " + detail));
    }

    private void check(String name, boolean ok, String detail) {
        if (ok) {
            pass(name, detail);
        } else {
            fail(name, detail);
        }
    }

    private void pass(String name, String detail) {
        passed++;
        System.out.println("PASS  " + name + (detail == null || detail.isBlank() ? "" : " — " + detail));
    }

    private void fail(String name, String detail) {
        failures.add(name);
        System.out.println("FAIL  " + name + " — " + detail);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
