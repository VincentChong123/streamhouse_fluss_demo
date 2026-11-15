package demo.paymentrisk.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The Fluss table contracts, defined once and used by the bootstrap (to create the tables through
 * the Fluss Admin API), the processing job (to build computed results), the ingestion job (to
 * validate computed results and build its INSERT statements) and the demo app (read-back).
 *
 * <p>Timestamp columns are {@code TIMESTAMP_LTZ(3)} in Fluss. Inside the Flink job they travel
 * as epoch-millisecond longs and are converted with {@code TO_TIMESTAMP_LTZ(x, 3)} in the
 * INSERT statement.
 */
public final class TableSchemas {
    public static final String DATABASE = "payment_risk";

    public enum ColumnType { STRING, INT, BIGINT, BOOLEAN, TIMESTAMP_LTZ_3 }

    public static final class Column {
        public final String name;
        public final ColumnType type;
        public final String comment;

        Column(String name, ColumnType type, String comment) {
            this.name = name;
            this.type = type;
            this.comment = comment;
        }
    }

    public static final class TableSpec {
        public final String name;
        public final String comment;
        public final List<Column> columns;
        public final List<String> primaryKey;

        TableSpec(String name, String comment, List<Column> columns, List<String> primaryKey) {
            this.name = name;
            this.comment = comment;
            this.columns = Collections.unmodifiableList(columns);
            this.primaryKey = Collections.unmodifiableList(primaryKey);
            for (String pk : primaryKey) {
                if (indexOf(pk) < 0) {
                    throw new IllegalArgumentException("primary key column missing: " + pk);
                }
            }
        }

        public int indexOf(String column) {
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).name.equals(column)) {
                    return i;
                }
            }
            return -1;
        }

        public List<String> columnNames() {
            List<String> names = new ArrayList<>();
            for (Column c : columns) {
                names.add(c.name);
            }
            return names;
        }
    }

    private static Builder table(String name, String comment) {
        return new Builder(name, comment);
    }

    private static final class Builder {
        private final String name;
        private final String comment;
        private final List<Column> cols = new ArrayList<>();

        Builder(String name, String comment) {
            this.name = name;
            this.comment = comment;
        }

        Builder col(String n, ColumnType t, String c) {
            cols.add(new Column(n, t, c));
            return this;
        }

        TableSpec pk(String... keys) {
            return new TableSpec(name, comment, cols, List.of(keys));
        }
    }

    private static final ColumnType S = ColumnType.STRING;
    private static final ColumnType I = ColumnType.INT;
    private static final ColumnType L = ColumnType.BIGINT;
    private static final ColumnType B = ColumnType.BOOLEAN;
    private static final ColumnType T = ColumnType.TIMESTAMP_LTZ_3;

    /** Latest supplier context per run and supplier, maintained by Flink. */
    public static final TableSpec SUPPLIER_RISK = table("supplier_risk",
            "Latest correlated supplier context per run (written only by the Flink ingestion job)")
            .col("run_id", S, "Scenario run identifier")
            .col("supplier_id", S, "Supplier identifier")
            .col("supplier_name", S, "Synthetic supplier name")
            .col("relationship_years", I, "Years of established relationship")
            .col("baseline_beneficiary_masked", S, "Established beneficiary account (masked)")
            .col("current_beneficiary_masked", S, "Beneficiary account after the latest change (masked)")
            .col("bank_change_id", S, "change_id of the latest beneficiary change")
            .col("bank_change_event_id", S, "event_id of the latest beneficiary change")
            .col("bank_change_occurred_at", T, "Business time of the latest beneficiary change")
            .col("bank_change_actor_id", S, "Actor who made the change")
            .col("bank_change_session_id", S, "Session in which the change was made")
            .col("identity_event_id", S, "Identity event linked to this change (decisive evidence)")
            .col("identity_device_id", S, "Device used for the linked modification")
            .col("identity_device_known", B, "Whether that device was previously known")
            .col("identity_occurred_at", T, "Business time of the linked identity activity")
            .col("unfamiliar_device_evidence", B, "Linked activity used a previously unseen device")
            .col("signal_band", S, "NORMAL | ELEVATED | HIGH, from the latest observed signals (not time-expired)")
            .col("signal_reason", S, "Why the band was assigned")
            .col("context_version", I, "Increments on each applied context change")
            .col("last_source_event_id", S, "Event that produced this version")
            .col("last_source_event_type", S, "Type of that event")
            .col("last_source_occurred_at", T, "Business time of that event")
            .col("updated_at", T, "Flink processing time when this version was produced")
            .col("computed_by", S, "Execution that computed this row (processing job or SQL statement)")
            .col("ingested_by", S, "Ingestion job execution that wrote this row into Fluss")
            .pk("run_id", "supplier_id");

    /** One logical review per run and payment, evaluated when the payment request arrives. */
    public static final TableSpec PAYMENT_REVIEWS = table("payment_reviews",
            "Deterministic payment-review results per run and payment (written only by the Flink ingestion job)")
            .col("run_id", S, "Scenario run identifier")
            .col("payment_id", S, "Payment identifier")
            .col("supplier_id", S, "Supplier identifier")
            .col("amount_minor", L, "Amount in minor units (cents)")
            .col("currency", S, "ISO-4217 currency")
            .col("payment_occurred_at", T, "Business time of the payment request")
            .col("outcome", S, "MANUAL_REVIEW | NO_RULE_MATCH | REVIEW_INCOMPLETE_CONTEXT (none authorizes payment)")
            .col("check_recent_change", S, "TRUE | FALSE | UNKNOWN")
            .col("check_unfamiliar_device", S, "TRUE | FALSE | UNKNOWN")
            .col("check_large_payment", S, "TRUE | FALSE | UNKNOWN")
            .col("seconds_since_bank_change", L, "payment.occurred_at - bank_change.occurred_at, seconds")
            .col("explanation", S, "Templated, deterministic explanation (not AI-generated)")
            .col("missing_evidence", S, "What evidence was missing, if any")
            .col("context_version_used", I, "supplier_risk.context_version the rule evaluated")
            .col("evidence_bank_change_event_id", S, "Evidence reference")
            .col("evidence_identity_event_id", S, "Evidence reference")
            .col("evidence_baseline_event_id", S, "Evidence reference")
            .col("source_payment_event_id", S, "The PAYMENT_REQUESTED event evaluated")
            .col("source_kafka_partition", I, "Kafka partition of that event")
            .col("source_kafka_offset", L, "Kafka offset of that event")
            .col("rule_version", S, "Rule identifier and version")
            .col("rule_parameters", S, "Window, threshold and currency used")
            .col("evaluated_at", T, "Flink processing time of the evaluation")
            .col("computed_by", S, "Execution that computed this row (processing job or SQL statement)")
            .col("ingested_by", S, "Ingestion job execution that wrote this row into Fluss")
            .pk("run_id", "payment_id");

    /** One receipt per consumed event: what the processing job did with it. */
    public static final TableSpec EVENT_RECEIPTS = table("event_receipts",
            "Per-event processing receipts (written only by the Flink ingestion job)")
            .col("run_id", S, "Scenario run identifier")
            .col("event_id", S, "Event identifier")
            .col("event_type", S, "Event type")
            .col("supplier_id", S, "Supplier identifier")
            .col("disposition", S, "APPLIED | APPLIED_FROM_BUFFER | BUFFERED_AWAITING_CHANGE | IGNORED_ACTOR_MISMATCH | REVIEWED | DUPLICATE_PAYMENT_IGNORED | REJECTED_INVALID")
            .col("detail", S, "Human-readable detail")
            .col("context_version_after", I, "supplier_risk.context_version after this event (or used, for payments)")
            .col("kafka_partition", I, "Kafka partition the event was read from")
            .col("kafka_offset", L, "Kafka offset the event was read from")
            .col("occurred_at", T, "Business time")
            .col("emitted_at", T, "Producer hand-off time")
            .col("processed_at", T, "Flink processing time")
            .col("computed_by", S, "Execution that computed this row (processing job or SQL statement)")
            .col("ingested_by", S, "Ingestion job execution that wrote this row into Fluss")
            .pk("run_id", "event_id");

    /** Columns filled in by the ingestion job, never by the computation that produced the row. */
    public static final List<String> PROVENANCE_COLUMNS = List.of("computed_by", "ingested_by");

    public static List<TableSpec> all() {
        return List.of(SUPPLIER_RISK, PAYMENT_REVIEWS, EVENT_RECEIPTS);
    }

    public static TableSpec byName(String name) {
        for (TableSpec t : all()) {
            if (t.name.equals(name)) {
                return t;
            }
        }
        return null;
    }

    private TableSchemas() {}
}
