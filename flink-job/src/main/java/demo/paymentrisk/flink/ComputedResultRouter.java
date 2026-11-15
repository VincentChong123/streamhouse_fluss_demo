package demo.paymentrisk.flink;

import demo.paymentrisk.common.ComputedResults;
import demo.paymentrisk.common.TableSchemas;
import demo.paymentrisk.common.TableSchemas.TableSpec;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Validates computed-result records of one run against {@link ComputedResults} and routes each
 * valid record, as a typed row, to the side output of its Fluss table. Invalid records are
 * logged and counted ({@code records_rejected}); they are never written.
 */
public class ComputedResultRouter extends ProcessFunction<Row, Row> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ComputedResultRouter.class);

    public static final Map<String, OutputTag<Row>> TAGS = new HashMap<>();

    static {
        for (TableSpec spec : TableSchemas.all()) {
            TAGS.put(spec.name, new OutputTag<>(spec.name, FlussSinks.rowType(spec)));
        }
    }

    private final String runId;
    private final String ingestionExecutionId;
    private transient LongCounter read;
    private transient LongCounter otherRuns;
    private transient LongCounter rejected;
    private transient Map<String, LongCounter> perTable;

    public ComputedResultRouter(String runId, String ingestionExecutionId) {
        this.runId = runId;
        this.ingestionExecutionId = ingestionExecutionId;
    }

    @Override
    public void open(OpenContext openContext) {
        read = counter("records_read");
        otherRuns = counter("records_other_runs");
        rejected = counter("records_rejected");
        perTable = new HashMap<>();
        for (TableSpec spec : TableSchemas.all()) {
            perTable.put(spec.name, counter("rows_" + spec.name));
        }
    }

    private LongCounter counter(String name) {
        LongCounter c = new LongCounter();
        getRuntimeContext().addAccumulator(name, c);
        return c;
    }

    @Override
    public void processElement(Row r, Context ctx, Collector<Row> out) {
        read.add(1);
        Long offset = (Long) r.getField("kafka_offset");
        ComputedResults.Parsed p = ComputedResults.parse((String) r.getField("record_value"));
        if (p.runId != null && !runId.equals(p.runId)) {
            otherRuns.add(1);
            return;
        }
        if (!p.ok()) {
            rejected.add(1);
            LOG.warn("Rejected computed result at offset {}: {}", offset, String.join("; ", p.errors));
            return;
        }
        ctx.output(TAGS.get(p.spec.name), Row.of(p.rowValues(p.executionId, ingestionExecutionId)));
        perTable.get(p.spec.name).add(1);
    }
}
