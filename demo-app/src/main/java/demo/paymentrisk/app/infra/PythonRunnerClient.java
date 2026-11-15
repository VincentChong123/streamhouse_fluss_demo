package demo.paymentrisk.app.infra;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.common.Json;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Client for the isolated python-runner service (see docker/python-runner). The runner executes
 * submitted code with the pinned pyfluss client, under time, memory and output limits.
 */
public final class PythonRunnerClient {
    private final String base;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    public PythonRunnerClient(String baseUrl) {
        this.base = baseUrl;
    }

    public JsonNode health() throws Exception {
        return call("GET", "/health", null, Duration.ofSeconds(3));
    }

    /** Runs code to completion (or until the runner's limits stop it). */
    public JsonNode run(String executionId, String code, int timeoutSeconds) throws Exception {
        ObjectNode b = Json.mapper().createObjectNode();
        b.put("execution_id", executionId);
        b.put("code", code);
        b.put("timeout_seconds", timeoutSeconds);
        return call("POST", "/run", b, Duration.ofSeconds(timeoutSeconds + 15L));
    }

    public JsonNode cancel(String executionId) throws Exception {
        ObjectNode b = Json.mapper().createObjectNode();
        b.put("execution_id", executionId);
        return call("POST", "/cancel", b, Duration.ofSeconds(5));
    }

    private JsonNode call(String method, String path, JsonNode body, Duration timeout) throws Exception {
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(base + path)).timeout(timeout);
        if (body != null) {
            rb.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(Json.write(body)));
        } else {
            rb.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> r = http.send(rb.build(), HttpResponse.BodyHandlers.ofString());
        JsonNode n = r.body() == null || r.body().isBlank() ? Json.mapper().createObjectNode() : Json.mapper().readTree(r.body());
        if (r.statusCode() / 100 != 2) {
            throw new IllegalStateException("python-runner HTTP " + r.statusCode() + ": " + n.path("error").asText(r.body()));
        }
        return n;
    }
}
