package demo.paymentrisk.app.playground;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import demo.paymentrisk.app.AppConfig;
import demo.paymentrisk.app.infra.PythonRunnerClient;
import demo.paymentrisk.app.run.ExecutionHistory;
import demo.paymentrisk.app.run.Run;
import demo.paymentrisk.app.run.RunManager;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Python executions, delegated to the isolated python-runner service (pyfluss 1.0.0, the
 * official Fluss Python client). Output is whatever the code printed; nothing is substituted.
 */
public final class PythonWorkspace {
    public static final int MAX_CODE = 20_000;
    private static final int MAX_EXECUTIONS = 20;
    public static final String CLIENT = "pyfluss 1.0.0 (Apache Fluss Python client), Python 3.12, python-runner container";

    public static final class Conflict extends Exception {
        public Conflict(String m) {
            super(m);
        }
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Execution {
        public String id;
        public String code;
        public String client = CLIENT;
        /** RUNNING | FINISHED | FAILED | TIMED_OUT | CANCELED */
        public volatile String status = "RUNNING";
        public volatile Integer exitCode;
        public volatile String stdout;
        public volatile String stderr;
        public volatile Long durationMs;
        public volatile boolean truncated;
        public volatile String error;
        public String startedAt = Instant.now().toString();
        public volatile String finishedAt;
        public volatile JsonNode limits;
    }

    private final AppConfig cfg;
    private final PythonRunnerClient runner;
    private final RunManager runs;
    private final ExecutionHistory history;
    private final Runnable onChange;
    private final List<Execution> executions = new CopyOnWriteArrayList<>();

    public PythonWorkspace(AppConfig cfg, PythonRunnerClient runner, RunManager runs, ExecutionHistory history, Runnable onChange) {
        this.cfg = cfg;
        this.runner = runner;
        this.runs = runs;
        this.history = history;
        this.onChange = onChange;
    }

    public List<Execution> executions() {
        return new ArrayList<>(executions);
    }

    public synchronized Execution submit(String code) throws Conflict {
        if (!cfg.pythonEnabled) {
            throw new Conflict("Python execution is disabled (PLAYGROUND_PYTHON_ENABLED=false).");
        }
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("The code is empty.");
        }
        if (code.length() > MAX_CODE) {
            throw new IllegalArgumentException("The code is longer than " + MAX_CODE + " characters.");
        }
        for (Execution x : executions) {
            if ("RUNNING".equals(x.status)) {
                throw new Conflict("A Python execution is already running (" + x.id + "). Cancel it or wait for it to finish.");
            }
        }
        Execution x = new Execution();
        x.id = "py-" + UUID.randomUUID().toString().substring(0, 8);
        x.code = code;
        executions.add(0, x);
        while (executions.size() > MAX_EXECUTIONS) {
            executions.remove(executions.size() - 1);
        }
        Run r = runs.current();
        ExecutionHistory.Entry h = history.add(x.id, "PYTHON", "Python (pyfluss) execution", r == null ? null : r.runId);
        Thread t = new Thread(() -> execute(x, h), "python-" + x.id);
        t.setDaemon(true);
        t.start();
        onChange.run();
        return x;
    }

    private void execute(Execution x, ExecutionHistory.Entry h) {
        long started = System.currentTimeMillis();
        try {
            JsonNode r = runner.run(x.id, x.code, cfg.pythonTimeoutSeconds);
            x.exitCode = r.hasNonNull("exit_code") ? r.get("exit_code").asInt() : null;
            x.stdout = r.path("stdout").asText("");
            x.stderr = r.path("stderr").asText("");
            x.durationMs = r.path("duration_ms").asLong(System.currentTimeMillis() - started);
            x.truncated = r.path("truncated").asBoolean(false);
            x.limits = r.path("limits");
            if (r.path("canceled").asBoolean(false)) {
                x.status = "CANCELED";
            } else if (r.path("timed_out").asBoolean(false)) {
                x.status = "TIMED_OUT";
                x.error = "Stopped after the " + cfg.pythonTimeoutSeconds + " s execution limit.";
            } else {
                x.status = Integer.valueOf(0).equals(x.exitCode) ? "FINISHED" : "FAILED";
                if (!"FINISHED".equals(x.status)) {
                    x.error = lastLine(x.stderr, "Exited with code " + x.exitCode);
                }
            }
        } catch (Exception e) {
            x.status = "FAILED";
            x.error = "The Python runner is not available: " + RunManager.root(e);
            x.durationMs = System.currentTimeMillis() - started;
        }
        x.finishedAt = Instant.now().toString();
        h.finish("FINISHED".equals(x.status) ? "SUCCEEDED" : "CANCELED".equals(x.status) ? "CANCELED" : "FAILED",
                x.error != null ? x.error : "exit code " + x.exitCode);
        onChange.run();
    }

    public boolean cancel(String id) {
        for (Execution x : executions) {
            if (x.id.equals(id) && "RUNNING".equals(x.status)) {
                try {
                    runner.cancel(id);
                    return true;
                } catch (Exception e) {
                    x.error = "Cancel failed: " + RunManager.root(e);
                    onChange.run();
                    return false;
                }
            }
        }
        return false;
    }

    private static String lastLine(String s, String def) {
        if (s == null || s.isBlank()) {
            return def;
        }
        String[] lines = s.strip().split("\n");
        return lines[lines.length - 1];
    }

    /** A working example for the current run. */
    public String example() {
        Run r = runs.current();
        String run = r == null ? "run-…" : r.runId;
        String sup = r == null ? "SUP-NORTHSTAR" : r.supplierId;
        String pay = r == null ? "PAY-NS-0001" : r.paymentId;
        return "# Reads this run's rows from Fluss with the official Python client (pyfluss 1.0.0).\n"
                + "# Primary-key lookups go directly to the Fluss TabletServer; no SQL engine is involved.\n"
                + "import asyncio, json, os, time\n"
                + "import fluss\n\n"
                + "RUN_ID = \"" + run + "\"\n"
                + "SUPPLIER_ID = \"" + sup + "\"\n"
                + "PAYMENT_ID = \"" + pay + "\"\n\n"
                + "async def lookup(conn, table, key):\n"
                + "    t = await conn.get_table(fluss.TablePath(\"" + cfg.flussDatabase + "\", table))\n"
                + "    started = time.perf_counter()\n"
                + "    row = await t.new_lookup().create_lookuper().lookup(key)\n"
                + "    print(f\"{table}: lookup took {(time.perf_counter() - started) * 1000:.1f} ms\")\n"
                + "    return row\n\n"
                + "async def main():\n"
                + "    config = fluss.Config({\"bootstrap.servers\": os.environ[\"FLUSS_BOOTSTRAP_SERVERS\"]})\n"
                + "    conn = await fluss.FlussConnection.create(config)\n"
                + "    try:\n"
                + "        supplier = await lookup(conn, \"supplier_risk\", {\"run_id\": RUN_ID, \"supplier_id\": SUPPLIER_ID})\n"
                + "        review = await lookup(conn, \"payment_reviews\", {\"run_id\": RUN_ID, \"payment_id\": PAYMENT_ID})\n"
                + "        print(json.dumps({\"supplier_risk\": supplier, \"payment_reviews\": review},\n"
                + "                         default=str, indent=2, ensure_ascii=False))\n"
                + "    finally:\n"
                + "        await conn.close()\n\n"
                + "asyncio.run(main())\n";
    }
}
