# Flink 1.20.0 + Fluss 1.0.0 connector, from the official Fluss quickstart image.
# Pinned by digest; see README "Tested compatibility matrix".
#
# Changes to the base image:
#  1. The official Flink Kafka SQL connector (3.3.0-1.20, checksum-verified) is added to
#     /opt/flink/lib. JobManager, TaskManager and SQL Gateway therefore share one copy of it:
#     the processing and ingestion jobs use it through Table API DDL, and SQL Workspace
#     statements use it through the SQL Gateway.
#  2. A checkpoint directory owned by the unprivileged `flink` user.
#  3. The OpenTelemetry Java Agent (2.34.0, checksum-verified) at /opt/otel for
#     zero-code instrumentation of JobManager and TaskManager.
FROM apache/fluss-quickstart-flink:1.20-1.0.0@sha256:70e67eeea65929cfb939aed89146212cfbda6dd4709aae9764fea1209072fcf9

ADD --chown=flink:flink \
    --checksum=sha256:1086f3eee73d727e234860fcd03adafc0d76f2fc70a25d39c36427693fff749d \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
    /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar

RUN mkdir -p /flink-checkpoints /opt/otel && chown flink:flink /flink-checkpoints /opt/otel

ADD --chown=flink:flink \
    --checksum=sha256:da44154cea128257779c8a782b9651fd7c0c6aa177cd3810f9a867ac8509555d \
    https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.34.0/opentelemetry-javaagent-2.34.0.jar \
    /opt/otel/opentelemetry-javaagent.jar
