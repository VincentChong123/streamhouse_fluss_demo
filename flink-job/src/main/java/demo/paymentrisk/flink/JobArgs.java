package demo.paymentrisk.flink;

import demo.paymentrisk.common.TableSchemas;
import demo.paymentrisk.common.rule.RuleConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Program arguments ({@code --key value}), supplied by the demo app when it submits a bounded
 * execution through the Flink REST API ({@code POST /jars/:id/run}).
 */
public final class JobArgs implements Serializable {
    private static final long serialVersionUID = 1L;

    /** {@code processing} or {@code ingestion}. */
    public String job;
    public String jobName;
    public String runId;
    public String executionId;

    public String kafkaBootstrap = "kafka:9092";
    /** The bounded input: one partition, offsets [startOffset, endOffset). */
    public String sourceTopic;
    public int sourcePartition = 0;
    public long startOffset = -1;
    public long endOffset = -1;

    /** Processing only: where computed results go. */
    public String outputTopic = "computed-results";
    public long thresholdMinor = RuleConfig.DEFAULT_THRESHOLD_MINOR;

    /** Ingestion only: the Fluss cluster and database. */
    public String flussBootstrap = "fluss-coordinator:9123";
    public String database = TableSchemas.DATABASE;

    public static JobArgs parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                String value = i + 1 < args.length && !args[i + 1].startsWith("--") ? args[++i] : "true";
                m.put(key, value);
            }
        }
        JobArgs a = new JobArgs();
        a.job = require(m, "job");
        a.runId = require(m, "run-id");
        a.executionId = require(m, "execution-id");
        a.jobName = m.getOrDefault("job-name", a.job + " " + a.executionId);
        a.kafkaBootstrap = m.getOrDefault("kafka-bootstrap", a.kafkaBootstrap);
        a.sourceTopic = require(m, "source-topic");
        a.sourcePartition = Integer.parseInt(m.getOrDefault("source-partition", "0"));
        a.startOffset = Long.parseLong(require(m, "start-offset"));
        a.endOffset = Long.parseLong(require(m, "end-offset"));
        if (a.startOffset < 0 || a.endOffset < a.startOffset) {
            throw new IllegalArgumentException("invalid offset range [" + a.startOffset + ", " + a.endOffset + ")");
        }
        a.outputTopic = m.getOrDefault("output-topic", a.outputTopic);
        a.thresholdMinor = Long.parseLong(m.getOrDefault("threshold-minor", String.valueOf(a.thresholdMinor)));
        a.flussBootstrap = m.getOrDefault("fluss-bootstrap", a.flussBootstrap);
        a.database = m.getOrDefault("database", a.database);
        return a;
    }

    private static String require(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("missing required argument --" + key);
        }
        return v;
    }

    public RuleConfig ruleConfig() {
        return new RuleConfig(thresholdMinor, RuleConfig.DEFAULT_CURRENCY, RuleConfig.DEFAULT_WINDOW);
    }

    @Override
    public String toString() {
        return "JobArgs{job=" + job + ", name=" + jobName + ", run=" + runId + ", execution=" + executionId
                + ", source=" + sourceTopic + "-" + sourcePartition + " [" + startOffset + ", " + endOffset + ")"
                + ", output=" + outputTopic + ", fluss=" + flussBootstrap + "/" + database
                + ", thresholdMinor=" + thresholdMinor + "}";
    }
}
