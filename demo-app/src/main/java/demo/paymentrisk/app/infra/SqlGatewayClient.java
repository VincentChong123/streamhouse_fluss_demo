package demo.paymentrisk.app.infra;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.common.Json;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Client for the Flink SQL Gateway REST endpoint (API v2). Statements are executed by the
 * gateway against the existing Flink session cluster; this class only submits them and fetches
 * their results.
 */
public final class SqlGatewayClient {
    private final String base;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    public SqlGatewayClient(String baseUrl) {
        this.base = baseUrl;
    }

    /** A failed gateway call, with the most useful line of the server-side error. */
    public static final class SqlError extends Exception {
        public final String summary;
        public final String detail;

        SqlError(String summary, String detail) {
            super(summary);
            this.summary = summary;
            this.detail = detail;
        }
    }

    public JsonNode info() throws Exception {
        return call("GET", "/v2/info", null, Duration.ofSeconds(3));
    }

    public String openSession(String name, Map<String, String> properties) throws Exception {
        ObjectNode b = Json.mapper().createObjectNode();
        b.put("sessionName", name);
        b.set("properties", Json.mapper().valueToTree(properties));
        return call("POST", "/v2/sessions", b, Duration.ofSeconds(15)).path("sessionHandle").asText();
    }

    public void closeSession(String session) {
        try {
            call("DELETE", "/v2/sessions/" + session, null, Duration.ofSeconds(10));
        } catch (Exception ignored) {
            // best effort: the gateway also expires idle sessions
        }
    }

    public String submit(String session, String statement, Duration executionTimeout) throws Exception {
        ObjectNode b = Json.mapper().createObjectNode();
        b.put("statement", statement);
        if (executionTimeout != null) {
            b.put("executionTimeout", executionTimeout.toMillis());
        }
        return call("POST", "/v2/sessions/" + session + "/statements", b, Duration.ofSeconds(60))
                .path("operationHandle").asText();
    }

    /** GET …/result/:token?rowFormat=JSON. Throws {@link SqlError} when the operation failed. */
    public JsonNode fetch(String session, String operation, long token) throws Exception {
        return call("GET", "/v2/sessions/" + session + "/operations/" + operation + "/result/" + token + "?rowFormat=JSON",
                null, Duration.ofSeconds(30));
    }

    public String status(String session, String operation) throws Exception {
        return call("GET", "/v2/sessions/" + session + "/operations/" + operation + "/status", null, Duration.ofSeconds(5))
                .path("status").asText();
    }

    public void cancel(String session, String operation) {
        try {
            call("POST", "/v2/sessions/" + session + "/operations/" + operation + "/cancel", null, Duration.ofSeconds(10));
        } catch (Exception ignored) {
            // the operation may already be finished
        }
    }

    public void closeOperation(String session, String operation) {
        try {
            call("DELETE", "/v2/sessions/" + session + "/operations/" + operation + "/close", null, Duration.ofSeconds(10));
        } catch (Exception ignored) {
            // the operation may already be closed
        }
    }

    /** Runs a statement that returns immediately (DDL) and waits for it to finish. */
    public void executeDdl(String session, String statement) throws Exception {
        String op = submit(session, statement, null);
        long deadline = System.currentTimeMillis() + 30_000;
        try {
            long token = 0;
            while (System.currentTimeMillis() < deadline) {
                JsonNode r = fetch(session, op, token);
                String type = r.path("resultType").asText();
                if ("EOS".equals(type) || ("PAYLOAD".equals(type) && r.path("nextResultUri").isMissingNode())) {
                    return;
                }
                if ("PAYLOAD".equals(type)) {
                    return; // DDL returns its single OK row as one payload
                }
                Thread.sleep(100);
            }
            throw new SqlError("SQL Gateway did not complete: " + statement, null);
        } finally {
            closeOperation(session, op);
        }
    }

    private JsonNode call(String method, String path, JsonNode body, Duration timeout) throws Exception {
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(base + path)).timeout(timeout);
        if (body != null) {
            rb.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(Json.write(body)));
        } else if ("POST".equals(method)) {
            rb.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString("{}"));
        } else {
            rb.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> r = http.send(rb.build(), HttpResponse.BodyHandlers.ofString());
        String text = r.body() == null ? "" : r.body();
        if (r.statusCode() / 100 != 2) {
            throw toError(r.statusCode(), text);
        }
        return text.isBlank() ? Json.mapper().createObjectNode() : Json.mapper().readTree(text);
    }

    private static SqlError toError(int status, String body) {
        List<String> causes = new ArrayList<>();
        String detail = body;
        try {
            JsonNode n = Json.mapper().readTree(body);
            StringBuilder all = new StringBuilder();
            for (JsonNode e : n.path("errors")) {
                all.append(e.asText()).append('\n');
            }
            detail = all.toString();
        } catch (Exception ignored) {
            // not JSON
        }
        for (String line : detail.split("\n")) {
            String l = line.trim();
            if (l.startsWith("Caused by: ")) {
                causes.add(l.substring("Caused by: ".length()));
            }
        }
        String summary = pickCause(causes);
        if (summary == null) {
            summary = "SQL Gateway HTTP " + status + ": " + firstLine(detail);
        }
        if (detail.length() > 8_000) {
            detail = detail.substring(0, 8_000) + "\n…";
        }
        return new SqlError(summary, detail);
    }

    /** The deepest cause that is not just a gateway wrapper ("Failed to execute the operation …"). */
    private static String pickCause(List<String> causes) {
        // Flink's own user-facing messages first: they name the line, column and object.
        for (String marker : List.of("ValidationException", "SqlParserException")) {
            for (String c : causes) {
                if (c.contains(marker)) {
                    return c.length() > 800 ? c.substring(0, 800) + "…" : c;
                }
            }
        }
        String best = null;
        for (String c : causes) {
            boolean wrapper = c.contains("SqlGatewayException") || c.contains("Failed to execute the operation")
                    || c.contains("Failed to fetchResults") || c.contains("Error while retrieving result")
                    || c.contains("Failed to fetch next result") || c.contains("Failed to fetch job execution result");
            if (!wrapper) {
                best = c;
            }
        }
        if (best == null && !causes.isEmpty()) {
            best = causes.get(causes.size() - 1);
        }
        if (best != null && best.length() > 800) {
            best = best.substring(0, 800) + "…";
        }
        return best;
    }

    private static String firstLine(String s) {
        int i = s.indexOf('\n');
        String l = i < 0 ? s : s.substring(0, i);
        return l.length() > 300 ? l.substring(0, 300) + "…" : l;
    }
}
