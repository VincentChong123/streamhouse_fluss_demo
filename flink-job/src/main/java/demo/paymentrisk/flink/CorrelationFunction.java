package demo.paymentrisk.flink;

import demo.paymentrisk.common.BusinessEvent;
import demo.paymentrisk.common.ComputedResults;
import demo.paymentrisk.common.OutputRow;
import demo.paymentrisk.common.correlation.CorrelationEngine;
import demo.paymentrisk.common.rule.RuleConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Keyed by {@code run_id|supplier_id}. Delegates all business logic to the pure
 * {@link CorrelationEngine} and emits every output row, in order, as a computed-result record
 * ({@code record_key}, {@code record_value}) for the {@code computed-results} topic.
 *
 * <p>The job is a bounded execution over one run's captured offsets, so keyed state lives only
 * for the duration of the execution.
 */
public class CorrelationFunction extends KeyedProcessFunction<String, BusinessEvent, Row> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CorrelationFunction.class);
    public static final String PRODUCER = "flink-processing-job";

    public static final TypeInformation<Row> OUTPUT_TYPE =
            new RowTypeInfo(new TypeInformation<?>[] {Types.STRING, Types.STRING}, new String[] {"record_key", "record_value"});

    private final RuleConfig rule;
    private final String executionId;
    private transient CorrelationEngine engine;
    private transient FlinkCorrelationState state;
    private transient LongCounter emitted;

    public CorrelationFunction(RuleConfig rule, String executionId) {
        this.rule = rule;
        this.executionId = executionId;
    }

    @Override
    public void open(OpenContext openContext) {
        state = new FlinkCorrelationState(
                getRuntimeContext().getState(new ValueStateDescriptor<>("supplier-context-json", Types.STRING)),
                getRuntimeContext().getMapState(new MapStateDescriptor<>("seen-event-ids", Types.STRING, Types.LONG)),
                getRuntimeContext().getMapState(new MapStateDescriptor<>("reviewed-payment-ids", Types.STRING, Types.STRING)),
                getRuntimeContext().getMapState(new MapStateDescriptor<>("pending-identity-json", Types.STRING, Types.STRING)));
        engine = new CorrelationEngine(rule);
        emitted = new LongCounter();
        getRuntimeContext().addAccumulator("results_emitted", emitted);
    }

    @Override
    public void processElement(BusinessEvent e, Context ctx, Collector<Row> out) {
        long now = ctx.timerService().currentProcessingTime();
        List<OutputRow> rows = engine.process(e, state, now);
        if (rows.isEmpty()) {
            LOG.info("Ignored duplicate event_id {} ({}) for key {}", e.eventId, e.eventType, ctx.getCurrentKey());
            return;
        }
        for (OutputRow r : rows) {
            if ("event_receipts".equals(r.table().name)) {
                LOG.info("{} {} offset={} -> {} (context v{}): {}", r.get("event_type"), r.get("event_id"),
                        r.get("kafka_offset"), r.get("disposition"), r.get("context_version_after"), r.get("detail"));
            }
            out.collect(Row.of(ComputedResults.key(r), ComputedResults.encode(r, executionId, PRODUCER)));
            emitted.add(1);
        }
    }
}
