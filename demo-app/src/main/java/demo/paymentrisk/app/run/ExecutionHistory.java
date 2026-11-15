package demo.paymentrisk.app.run;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Every operation a user started (stage operations, manual sends, SQL statements, Python runs),
 * newest first, bounded. Entries are updated in place while the operation runs.
 */
public final class ExecutionHistory {
    public static final int MAX = 200;

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Entry {
        public String id;
        /** SCENARIO_EVENTS | EVENT_SEND | PROCESSING_JOB | INGESTION_JOB | FLUSS_LOOKUP | SQL_STATEMENT | PYTHON */
        public String kind;
        public String label;
        public String runId;
        /** RUNNING | SUCCEEDED | FAILED | CANCELED */
        public volatile String status = "RUNNING";
        public String startedAt = Instant.now().toString();
        public volatile String finishedAt;
        public volatile Long durationMs;
        public volatile String jobId;
        public volatile String jobUrl;
        public volatile String detail;

        public void finish(String s, String detailText) {
            status = s;
            if (detailText != null) {
                detail = detailText;
            }
            finishedAt = Instant.now().toString();
            durationMs = Instant.parse(finishedAt).toEpochMilli() - Instant.parse(startedAt).toEpochMilli();
        }
    }

    private final List<Entry> entries = new CopyOnWriteArrayList<>();
    private final Runnable onChange;

    public ExecutionHistory(Runnable onChange) {
        this.onChange = onChange;
    }

    public synchronized Entry add(String id, String kind, String label, String runId) {
        Entry e = new Entry();
        e.id = id;
        e.kind = kind;
        e.label = label;
        e.runId = runId;
        entries.add(0, e);
        while (entries.size() > MAX) {
            entries.remove(entries.size() - 1);
        }
        onChange.run();
        return e;
    }

    public List<Entry> list() {
        return new ArrayList<>(entries);
    }
}
