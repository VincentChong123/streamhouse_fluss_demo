package demo.paymentrisk.flink;

/**
 * Table API DDL for the Kafka connector (flink-sql-connector-kafka in /opt/flink/lib). The jobs
 * use these tables instead of the DataStream Kafka API so that the jobs, the SQL Gateway and the
 * SQL Workspace all share the one connector on the cluster's classpath.
 */
public final class KafkaTables {
    private KafkaTables() {}

    /**
     * A bounded table over one partition, offsets [start, end): the captured range of a run.
     * Columns: {@code record_value} plus the Kafka partition, offset and timestamp.
     */
    public static String boundedSource(String table, String topic, String bootstrap, int partition,
                                       long start, long end, String groupId) {
        return "CREATE TEMPORARY TABLE `" + table + "` (\n"
                + "  `record_value` STRING,\n"
                + "  `kafka_partition` INT METADATA FROM 'partition' VIRTUAL,\n"
                + "  `kafka_offset` BIGINT METADATA FROM 'offset' VIRTUAL,\n"
                + "  `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL\n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = '" + topic + "',\n"
                + "  'properties.bootstrap.servers' = '" + bootstrap + "',\n"
                + "  'properties.group.id' = '" + groupId + "',\n"
                + "  'scan.startup.mode' = 'specific-offsets',\n"
                + "  'scan.startup.specific-offsets' = 'partition:" + partition + ",offset:" + start + "',\n"
                + "  'scan.bounded.mode' = 'specific-offsets',\n"
                + "  'scan.bounded.specific-offsets' = 'partition:" + partition + ",offset:" + end + "',\n"
                + "  'format' = 'raw'\n"
                + ")";
    }

    /** A key/value sink: both columns are written as raw UTF-8 strings. */
    public static String keyValueSink(String table, String topic, String bootstrap) {
        return "CREATE TEMPORARY TABLE `" + table + "` (\n"
                + "  `record_key` STRING,\n"
                + "  `record_value` STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = '" + topic + "',\n"
                + "  'properties.bootstrap.servers' = '" + bootstrap + "',\n"
                + "  'key.format' = 'raw',\n"
                + "  'key.fields' = 'record_key',\n"
                + "  'value.format' = 'raw',\n"
                + "  'value.fields-include' = 'EXCEPT_KEY',\n"
                + "  'sink.delivery-guarantee' = 'at-least-once'\n"
                + ")";
    }
}
