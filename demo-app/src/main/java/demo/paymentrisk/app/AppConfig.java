package demo.paymentrisk.app;

import demo.paymentrisk.common.TableSchemas;
import demo.paymentrisk.common.rule.RuleConfig;

import java.nio.file.Path;

/** All settings come from environment variables (see compose.yaml and docs/configuration.md). */
public final class AppConfig {
    public final String kafkaBootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092");
    /** Stage 1 input: Northstar business events (validated contract). */
    public final String inputTopic = env("KAFKA_TOPIC", "business-events");
    /** Boundary between the processing job and the ingestion job. */
    public final String computedTopic = env("KAFKA_COMPUTED_TOPIC", "computed-results");
    /** Free-form JSON records for the SQL Workspace. */
    public final String playgroundTopic = env("KAFKA_PLAYGROUND_TOPIC", "playground-events");

    public final String flussBootstrap = env("FLUSS_BOOTSTRAP", "fluss-coordinator:9123");
    public final String flussDatabase = env("FLUSS_DATABASE", TableSchemas.DATABASE);

    public final String flinkRestUrl = stripSlash(env("FLINK_REST_URL", "http://flink-jobmanager:8081"));
    public final String flinkUiPublicUrl = stripSlash(env("FLINK_UI_PUBLIC_URL", "http://localhost:8081"));
    public final Path flinkJobJar = Path.of(env("FLINK_JOB_JAR", "/app/flink-job.jar"));
    public final String flinkEntryClass = "demo.paymentrisk.flink.PaymentRiskJob";

    public final String sqlGatewayUrl = stripSlash(env("SQL_GATEWAY_URL", "http://flink-sql-gateway:8083"));
    public final String pythonRunnerUrl = stripSlash(env("PYTHON_RUNNER_URL", "http://python-runner:8000"));
    public final boolean pythonEnabled = Boolean.parseBoolean(env("PLAYGROUND_PYTHON_ENABLED", "true"));
    /** When set, SQL and Python execution require {@code Authorization: Bearer <token>}. */
    public final String accessToken = env("PLAYGROUND_ACCESS_TOKEN", "");

    public final Path bootstrapStatusFile = Path.of(env("BOOTSTRAP_STATUS_FILE", "/status/bootstrap.json"));
    public final long ruleThresholdMinor = Long.parseLong(env("RULE_LARGE_PAYMENT_THRESHOLD_MINOR",
            String.valueOf(RuleConfig.DEFAULT_THRESHOLD_MINOR)));

    public final int httpPort = Integer.parseInt(env("DEMO_HTTP_PORT", "8090"));
    public final String uiVersion = env("DEMO_UI_VERSION", "v2");
    public final Path webDir = Path.of(env("DEMO_WEB_DIR", "/app/web"));
    public final Path assetsDir = Path.of(env("DEMO_ASSETS_DIR", "/app/assets"));
    public final Path scenariosDir = Path.of(env("DEMO_SCENARIOS_DIR", "/app/scenarios"));
    public final String scenarioId = env("DEMO_SCENARIO", "northstar-suspicious");

    /** Rendering pace between scenario events (each event still waits for its real ack). */
    public final long emitPaceMs = Long.parseLong(env("DEMO_EMIT_PACE_MS", "150"));
    public final long sendTimeoutMs = Long.parseLong(env("DEMO_SEND_TIMEOUT_MS", "15000"));
    /** Upper bound for a bounded processing/ingestion execution before it is cancelled. */
    public final long stageJobTimeoutMs = Long.parseLong(env("STAGE_JOB_TIMEOUT_MS", "120000"));
    /** How often a running stage job's state is read from the Flink REST API. */
    public final long jobPollMs = Long.parseLong(env("STAGE_JOB_POLL_MS", "100"));

    public final long sqlBatchTimeoutMs = Long.parseLong(env("SQL_BATCH_TIMEOUT_MS", "90000"));
    public final long sqlStreamingMaxMs = Long.parseLong(env("SQL_STREAMING_MAX_MS", "300000"));
    public final int sqlMaxRows = Integer.parseInt(env("SQL_MAX_ROWS", "500"));
    public final int sqlMaxConcurrent = Integer.parseInt(env("SQL_MAX_CONCURRENT", "2"));
    public final int pythonTimeoutSeconds = Integer.parseInt(env("PYTHON_TIMEOUT_SECONDS", "20"));

    public final String demoAppUrl = stripSlash(env("DEMO_APP_URL", "http://demo-app:8090"));

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v.trim();
    }

    private static String stripSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }
}
