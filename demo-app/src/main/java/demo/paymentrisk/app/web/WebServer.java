package demo.paymentrisk.app.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import demo.paymentrisk.app.AppConfig;
import demo.paymentrisk.app.infra.FlussGateway;
import demo.paymentrisk.app.playground.EventsWorkspace;
import demo.paymentrisk.app.playground.PythonWorkspace;
import demo.paymentrisk.app.playground.SqlPolicy;
import demo.paymentrisk.app.playground.SqlWorkspace;
import demo.paymentrisk.app.run.Run;
import demo.paymentrisk.app.run.RunManager;
import demo.paymentrisk.common.Json;
import demo.paymentrisk.common.TableSchemas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Plain JDK HTTP server: the buildless UI, a JSON API and SSE snapshots. GET requests never
 * start anything; every operation is an explicit POST.
 */
public final class WebServer {
    private static final Logger LOG = LoggerFactory.getLogger(WebServer.class);
    private static final Pattern UI_FILE = Pattern.compile("v\\d+\\.html");
    private static final Pattern ASSET = Pattern.compile("[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*");
    private static final Pattern STAGE = Pattern.compile("/api/stages/([1-4])");
    private static final Pattern CANCEL = Pattern.compile("/api/(sql|python)/executions/([A-Za-z0-9-]+)/cancel");
    private static final String CSP = "default-src 'self'; script-src 'self' 'unsafe-inline'; "
            + "style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'";

    private final AppConfig cfg;
    private final RunManager runs;
    private final EventsWorkspace events;
    private final SqlWorkspace sql;
    private final PythonWorkspace python;
    private final FlussGateway fluss;
    private final SseHub sse;
    private final Supplier<String> snapshot;
    private HttpServer server;

    public WebServer(AppConfig cfg, RunManager runs, EventsWorkspace events, SqlWorkspace sql, PythonWorkspace python,
                     FlussGateway fluss, SseHub sse, Supplier<String> snapshot) {
        this.cfg = cfg;
        this.runs = runs;
        this.events = events;
        this.sql = sql;
        this.python = python;
        this.fluss = fluss;
        this.sse = sse;
        this.snapshot = snapshot;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(cfg.httpPort), 64);
        server.setExecutor(Executors.newFixedThreadPool(16));
        server.createContext("/", this::handle);
        server.start();
        LOG.info("Demo UI listening on :{} (serving web/{}.html at /)", cfg.httpPort, cfg.uiVersion);
    }

    private void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        String method = ex.getRequestMethod();
        try {
            Matcher stage = STAGE.matcher(path);
            Matcher cancel = CANCEL.matcher(path);
            if ("GET".equals(method) && ("/".equals(path) || "/index.html".equals(path))) {
                serveFile(ex, cfg.webDir.resolve(cfg.uiVersion + ".html"));
            } else if ("GET".equals(method) && path.startsWith("/web/") && UI_FILE.matcher(path.substring(5)).matches()) {
                serveFile(ex, cfg.webDir.resolve(path.substring(5)));
            } else if ("GET".equals(method) && path.startsWith("/assets/") && ASSET.matcher(path.substring(8)).matches()
                    && !path.contains("..")) {
                serveFile(ex, cfg.assetsDir.resolve(path.substring(8)));
            } else if ("GET".equals(method) && "/healthz".equals(path)) {
                text(ex, 200, "ok");
            } else if ("GET".equals(method) && "/api/state".equals(path)) {
                json(ex, 200, snapshot.get());
            } else if ("GET".equals(method) && "/api/stream".equals(path)) {
                sse.register(ex);
                return; // the exchange stays open for SSE
            } else if ("POST".equals(method) && "/api/runs".equals(path)) {
                Run r = runs.newRun();
                json(ex, 201, Json.write(Json.mapper().createObjectNode().put("run_id", r.runId)));
            } else if ("POST".equals(method) && stage.matches()) {
                startStage(ex, Integer.parseInt(stage.group(1)));
            } else if ("GET".equals(method) && "/api/events/templates".equals(path)) {
                json(ex, 200, Json.write(events.templates()));
            } else if ("POST".equals(method) && "/api/events".equals(path)) {
                sendEvents(ex);
            } else if ("GET".equals(method) && "/api/sql/ddl".equals(path)) {
                ObjectNode d = Json.mapper().createObjectNode();
                d.set("ddl", Json.mapper().valueToTree(sql.sessionDdl()));
                json(ex, 200, Json.write(d));
            } else if ("GET".equals(method) && "/api/sql/examples".equals(path)) {
                json(ex, 200, Json.write(sql.examples()));
            } else if ("POST".equals(method) && "/api/sql/executions".equals(path)) {
                submitSql(ex);
            } else if ("GET".equals(method) && "/api/python/example".equals(path)) {
                json(ex, 200, Json.write(Json.mapper().createObjectNode().put("code", python.example())));
            } else if ("POST".equals(method) && "/api/python/executions".equals(path)) {
                submitPython(ex);
            } else if ("POST".equals(method) && cancel.matches()) {
                if (!authorized(ex)) {
                    return;
                }
                boolean ok = "sql".equals(cancel.group(1)) ? sql.cancel(cancel.group(2)) : python.cancel(cancel.group(2));
                json(ex, ok ? 202 : 409, Json.write(Json.mapper().createObjectNode().put("cancel_requested", ok)
                        .put("error", ok ? null : "No running execution with that id.")));
            } else if ("GET".equals(method) && path.startsWith("/api/lookup/")) {
                lookup(ex, path.substring("/api/lookup/".length()));
            } else {
                text(ex, 404, "not found");
            }
        } catch (RunManager.ConflictException c) {
            error(ex, 409, c.getMessage());
        } catch (IllegalArgumentException e) {
            error(ex, 400, e.getMessage());
        } catch (Exception e) {
            LOG.warn("{} {} failed: {}", method, path, e.toString());
            error(ex, 500, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void startStage(HttpExchange ex, int n) throws Exception {
        switch (n) {
            case 1: runs.emitScenario(); break;
            case 2: runs.runProcessing(); break;
            case 3: runs.runIngestion(); break;
            default: runs.query(); break;
        }
        json(ex, 202, Json.write(Json.mapper().createObjectNode().put("stage", n).put("run_id", runs.current().runId)
                .put("status", "RUNNING")));
    }

    private void sendEvents(HttpExchange ex) throws IOException {
        JsonNode b = body(ex);
        try {
            List<EventsWorkspace.Sent> sent = events.send(b.path("topic").asText(null), b.path("key").asText(null),
                    b.path("payload").asText(null), b.path("batch").asBoolean(false));
            boolean allOk = sent.stream().allMatch(s -> "ACKNOWLEDGED".equals(s.status));
            ObjectNode r = Json.mapper().createObjectNode();
            r.set("records", Json.mapper().valueToTree(sent));
            if (!allOk) {
                r.put("error", "Kafka did not acknowledge every record; see each record's error.");
            }
            json(ex, allOk ? 200 : 502, Json.write(r));
        } catch (EventsWorkspace.InvalidRequest e) {
            ObjectNode r = Json.mapper().createObjectNode().put("error", e.getMessage());
            r.set("record_errors", Json.mapper().valueToTree(e.errors));
            json(ex, 400, Json.write(r));
        }
    }

    private void submitSql(HttpExchange ex) throws IOException {
        if (!authorized(ex)) {
            return;
        }
        JsonNode b = body(ex);
        try {
            SqlWorkspace.Execution x = sql.submit(b.path("statement").asText(null), b.path("mode").asText("batch"),
                    b.path("workspace").asText("sql"));
            json(ex, 202, Json.write(Json.mapper().createObjectNode().put("execution_id", x.id).put("kind", x.kind)));
        } catch (SqlPolicy.Rejected e) {
            error(ex, 400, e.getMessage());
        } catch (SqlWorkspace.Conflict e) {
            error(ex, 409, e.getMessage());
        }
    }

    private void submitPython(HttpExchange ex) throws IOException {
        if (!authorized(ex)) {
            return;
        }
        JsonNode b = body(ex);
        try {
            PythonWorkspace.Execution x = python.submit(b.path("code").asText(null));
            json(ex, 202, Json.write(Json.mapper().createObjectNode().put("execution_id", x.id)));
        } catch (PythonWorkspace.Conflict e) {
            error(ex, 409, e.getMessage());
        }
    }

    /** SQL and Python execution require the access token when PLAYGROUND_ACCESS_TOKEN is set. */
    private boolean authorized(HttpExchange ex) throws IOException {
        if (cfg.accessToken.isEmpty()) {
            return true;
        }
        String h = ex.getRequestHeaders().getFirst("Authorization");
        String given = h != null && h.startsWith("Bearer ") ? h.substring(7).trim() : "";
        if (MessageDigest.isEqual(given.getBytes(StandardCharsets.UTF_8), cfg.accessToken.getBytes(StandardCharsets.UTF_8))) {
            return true;
        }
        error(ex, 401, "This deployment requires an access token for SQL and Python execution.");
        return false;
    }

    /** GET /api/lookup/{table}?run_id=..&supplier_id=.. : one native primary-key lookup, right now. */
    private void lookup(HttpExchange ex, String table) throws IOException {
        TableSchemas.TableSpec spec = TableSchemas.byName(table);
        if (spec == null) {
            error(ex, 404, "unknown table " + table + "; use supplier_risk, payment_reviews or event_receipts");
            return;
        }
        Map<String, String> q = query(ex);
        List<String> key = new ArrayList<>();
        for (String pk : spec.primaryKey) {
            String v = q.get(pk);
            if (v == null || v.isBlank()) {
                error(ex, 400, "query parameters required: " + spec.primaryKey);
                return;
            }
            key.add(v);
        }
        FlussGateway.Observation o = fluss.lookup(spec, key, Duration.ofSeconds(3));
        json(ex, !o.ok ? 503 : o.found ? 200 : 404, Json.write(o));
    }

    // ------------------------------------------------------------------ helpers

    private static JsonNode body(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            byte[] b = in.readNBytes(256 * 1024);
            try {
                return b.length == 0 ? Json.mapper().createObjectNode() : Json.mapper().readTree(b);
            } catch (Exception e) {
                throw new IllegalArgumentException("Request body is not valid JSON.");
            }
        }
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> m = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw == null) {
            return m;
        }
        for (String pair : raw.split("&")) {
            int i = pair.indexOf('=');
            if (i > 0) {
                m.put(URLDecoder.decode(pair.substring(0, i), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8));
            }
        }
        return m;
    }

    private void serveFile(HttpExchange ex, Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            text(ex, 404, "not found: " + file.getFileName());
            return;
        }
        String name = file.getFileName().toString();
        String type = name.endsWith(".html") ? "text/html; charset=utf-8"
                : name.endsWith(".svg") ? "image/svg+xml"
                : name.endsWith(".png") ? "image/png"
                : name.endsWith(".css") ? "text/css; charset=utf-8"
                : name.endsWith(".js") ? "text/javascript; charset=utf-8"
                : name.endsWith(".json") ? "application/json" : "application/octet-stream";
        byte[] b = Files.readAllBytes(file);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.getResponseHeaders().set("Cache-Control", name.endsWith(".html") ? "no-store" : "max-age=3600");
        ex.getResponseHeaders().set("Content-Security-Policy", CSP);
        ex.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        ex.sendResponseHeaders(200, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    private static void error(HttpExchange ex, int status, String message) throws IOException {
        json(ex, status, Json.write(Json.mapper().createObjectNode().put("error", message)));
    }

    private static void json(HttpExchange ex, int status, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(status, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    private static void text(HttpExchange ex, int status, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(status, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }
}
