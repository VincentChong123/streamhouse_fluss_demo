package demo.paymentrisk.app.infra;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.paymentrisk.common.Json;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Minimal client for the Flink REST API (the same API the Flink web UI uses). */
public final class FlinkRest {
    /** Job states in which a job still exists and must not be submitted again. */
    public static final Set<String> NON_TERMINAL = Set.of(
            "INITIALIZING", "CREATED", "RUNNING", "RESTARTING", "FAILING", "CANCELLING", "RECONCILING", "SUSPENDED");

    private final String base;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    public FlinkRest(String baseUrl) {
        this.base = baseUrl;
    }

    public static final class JobSummary {
        public String id;
        public String name;
        public String state;
        public long startTime;
        public int tasksTotal;
        public int tasksRunning;

        public boolean allTasksRunning() {
            return "RUNNING".equals(state) && tasksTotal > 0 && tasksRunning == tasksTotal;
        }
    }

    public JsonNode overview() throws Exception {
        return get("/overview");
    }

    public List<JobSummary> jobs() throws Exception {
        List<JobSummary> out = new ArrayList<>();
        for (JsonNode j : get("/jobs/overview").path("jobs")) {
            JobSummary s = new JobSummary();
            s.id = j.path("jid").asText();
            s.name = j.path("name").asText();
            s.state = j.path("state").asText();
            s.startTime = j.path("start-time").asLong();
            s.tasksTotal = j.path("tasks").path("total").asInt();
            s.tasksRunning = j.path("tasks").path("running").asInt();
            out.add(s);
        }
        return out;
    }

    /** Jobs with this name that still exist (not FINISHED/CANCELED/FAILED). */
    public List<JobSummary> activeJobs(String name) throws Exception {
        List<JobSummary> out = new ArrayList<>();
        for (JobSummary s : jobs()) {
            if (name.equals(s.name) && NON_TERMINAL.contains(s.state)) {
                out.add(s);
            }
        }
        return out;
    }

    public JsonNode checkpoints(String jobId) throws Exception {
        return get("/jobs/" + jobId + "/checkpoints");
    }

    /** GET /jobs/:id — state, timestamps, duration and vertices. */
    public JsonNode job(String jobId) throws Exception {
        return get("/jobs/" + jobId);
    }

    /** User accumulators of a job (counters the jobs register), name to value. */
    public Map<String, String> accumulators(String jobId) throws Exception {
        Map<String, String> m = new TreeMap<>();
        for (JsonNode a : get("/jobs/" + jobId + "/accumulators").path("user-task-accumulators")) {
            m.put(a.path("name").asText(), a.path("value").asText());
        }
        return m;
    }

    /** The most specific cause of a failed job, from /jobs/:id/exceptions (null if none). */
    public String rootCause(String jobId) {
        try {
            JsonNode ex = get("/jobs/" + jobId + "/exceptions");
            String trace = ex.path("root-exception").asText("");
            JsonNode hist = ex.path("exceptionHistory").path("entries");
            if (trace.isEmpty() && hist.size() > 0) {
                trace = hist.get(0).path("stacktrace").asText("");
            }
            return trace.isEmpty() ? null : summarize(trace);
        } catch (Exception e) {
            return null;
        }
    }

    /** First line plus the deepest "Caused by" line of a Java stack trace. */
    public static String summarize(String trace) {
        String first = null;
        String deepest = null;
        for (String line : trace.split("\n")) {
            String l = line.trim();
            if (first == null && !l.isEmpty()) {
                first = l;
            }
            if (l.startsWith("Caused by: ")) {
                deepest = l.substring("Caused by: ".length());
            }
        }
        String s = deepest != null ? deepest : first;
        return s == null ? null : s.length() > 600 ? s.substring(0, 600) + "…" : s;
    }

    /** IDs of uploaded jars with this file name (GET /jars). */
    public List<String> jarIds(String fileName) throws Exception {
        List<String> ids = new ArrayList<>();
        for (JsonNode f : get("/jars").path("files")) {
            if (fileName.equals(f.path("name").asText())) {
                ids.add(f.path("id").asText());
            }
        }
        return ids;
    }

    public String jobUrl(String publicBase, String jobId) {
        return publicBase + "/#/job/completed/" + jobId + "/overview";
    }

    /** POST /jars/upload (multipart, field "jarfile"). Returns the jar ID. */
    public String uploadJar(Path jar) throws Exception {
        String boundary = "----demo" + UUID.randomUUID().toString().replace("-", "");
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(("--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + jar.getFileName() + "\"\r\n"
                + "Content-Type: application/x-java-archive\r\n\r\n").getBytes(StandardCharsets.UTF_8));
        body.write(Files.readAllBytes(jar));
        body.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + "/jars/upload"))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
                .build();
        JsonNode r = send(req);
        String filename = r.path("filename").asText();
        if (filename.isEmpty()) {
            throw new IllegalStateException("Flink jar upload returned no filename: " + r);
        }
        return filename.substring(filename.lastIndexOf('/') + 1);
    }

    /** POST /jars/{id}/run. Returns the job ID. */
    public String runJar(String jarId, String entryClass, List<String> args, int parallelism,
                         String savepointPath) throws Exception {
        ObjectNode b = Json.mapper().createObjectNode();
        b.put("entryClass", entryClass);
        b.put("parallelism", parallelism);
        b.set("programArgsList", Json.mapper().valueToTree(args));
        if (savepointPath != null) {
            b.put("savepointPath", savepointPath);
            b.put("allowNonRestoredState", true);
        }
        JsonNode r = post("/jars/" + jarId + "/run", b, Duration.ofSeconds(120));
        String id = r.path("jobid").asText();
        if (id.isEmpty()) {
            throw new IllegalStateException("Flink did not return a job id: " + r);
        }
        return id;
    }

    /** Stop-with-savepoint; returns the savepoint location. */
    public String stopWithSavepoint(String jobId, String targetDirectory, Duration timeout) throws Exception {
        ObjectNode b = Json.mapper().createObjectNode();
        b.put("targetDirectory", targetDirectory);
        b.put("drain", false);
        String trigger = post("/jobs/" + jobId + "/stop", b, Duration.ofSeconds(30)).path("request-id").asText();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            JsonNode s = get("/jobs/" + jobId + "/savepoints/" + trigger);
            if ("COMPLETED".equals(s.path("status").path("id").asText())) {
                JsonNode op = s.path("operation");
                if (op.has("failure-cause")) {
                    throw new IllegalStateException("savepoint failed: " + op.path("failure-cause").path("stack-trace").asText("").lines().findFirst().orElse("?"));
                }
                return op.path("location").asText();
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("savepoint did not complete within " + timeout);
    }

    public void cancel(String jobId) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + "/jobs/" + jobId + "?mode=cancel"))
                .timeout(Duration.ofSeconds(10))
                .method("PATCH", HttpRequest.BodyPublishers.noBody())
                .build();
        send(req);
    }

    private JsonNode get(String path) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(5)).GET().build());
    }

    private JsonNode post(String path, JsonNode body, Duration timeout) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(base + path))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(Json.write(body)))
                .build());
    }

    private JsonNode send(HttpRequest req) throws Exception {
        HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() / 100 != 2) {
            String body = r.body() == null ? "" : r.body();
            throw new IllegalStateException("Flink REST " + req.method() + " " + req.uri().getPath() + " -> HTTP "
                    + r.statusCode() + ": " + (body.length() > 600 ? body.substring(0, 600) + "..." : body));
        }
        return r.body() == null || r.body().isBlank() ? Json.mapper().createObjectNode() : Json.mapper().readTree(r.body());
    }
}
