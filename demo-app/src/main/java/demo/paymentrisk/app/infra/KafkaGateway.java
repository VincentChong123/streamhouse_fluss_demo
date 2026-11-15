package demo.paymentrisk.app.infra;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Admin + an idempotent producer. Every result here is directly observed from the broker.
 *
 * <p>Clients are created on first use, not in the constructor: while the {@code kafka} container
 * is stopped its Compose DNS name does not resolve, and Kafka clients refuse to be constructed
 * ("No resolvable bootstrap urls"). Creating them lazily turns that into a failed operation
 * instead of a failed process, and the next call retries.
 */
public final class KafkaGateway implements AutoCloseable {
    private final String bootstrap;
    private Admin admin;
    private KafkaProducer<String, String> producer;

    public KafkaGateway(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    private synchronized Admin admin() {
        if (admin == null) {
            Properties p = new Properties();
            p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            p.put(AdminClientConfig.CLIENT_ID_CONFIG, "demo-app-admin");
            p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
            p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 8_000);
            p.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 2_000);
            admin = Admin.create(p);
        }
        return admin;
    }

    public static final class ClusterInfo {
        public final String clusterId;
        public final int brokers;

        ClusterInfo(String clusterId, int brokers) {
            this.clusterId = clusterId;
            this.brokers = brokers;
        }
    }

    public ClusterInfo describeCluster(Duration timeout) throws Exception {
        DescribeClusterResult r = admin().describeCluster();
        long ms = timeout.toMillis();
        return new ClusterInfo(r.clusterId().get(ms, TimeUnit.MILLISECONDS), r.nodes().get(ms, TimeUnit.MILLISECONDS).size());
    }

    /** @return the number of partitions, or -1 if the topic does not exist. */
    public int partitions(String topic, Duration timeout) throws Exception {
        try {
            Map<String, TopicDescription> d = admin().describeTopics(List.of(topic)).allTopicNames()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return d.get(topic).partitions().size();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return -1;
            }
            throw e;
        }
    }

    /** @return true if created now, false if it already existed. */
    public boolean createTopic(String topic, int partitions, short replication, Duration timeout) throws Exception {
        NewTopic t = new NewTopic(topic, partitions, replication)
                .configs(Map.of("min.insync.replicas", "1", "retention.ms", String.valueOf(Duration.ofDays(7).toMillis())));
        try {
            admin().createTopics(List.of(t)).all().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                return false;
            }
            throw e;
        }
    }

    public static final class Ack {
        public final int partition;
        public final long offset;
        public final long kafkaTimestampMs;
        public final long sendStartedMs;
        public final long ackedAtMs;

        Ack(int partition, long offset, long kafkaTimestampMs, long sendStartedMs, long ackedAtMs) {
            this.partition = partition;
            this.offset = offset;
            this.kafkaTimestampMs = kafkaTimestampMs;
            this.sendStartedMs = sendStartedMs;
            this.ackedAtMs = ackedAtMs;
        }
    }

    private synchronized KafkaProducer<String, String> producer() {
        if (producer == null) {
            Properties p = new Properties();
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            p.put(ProducerConfig.CLIENT_ID_CONFIG, "demo-app-producer");
            p.put(ProducerConfig.ACKS_CONFIG, "all");
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
            p.put(ProducerConfig.LINGER_MS_CONFIG, 0);
            p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
            p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
            p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 12_000);
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer = new KafkaProducer<>(p);
        }
        return producer;
    }

    /** Fetches topic metadata once so the first real send is not inflated by connection setup. */
    public void warmUp(String topic) {
        producer().partitionsFor(topic);
    }

    /** Sends one record and blocks until the broker acknowledges it (acks=all) or fails. */
    public Ack send(String topic, String key, String value, Duration timeout) throws Exception {
        long started = System.currentTimeMillis();
        RecordMetadata md = producer().send(new ProducerRecord<>(topic, key, value))
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        return new Ack(md.partition(), md.offset(), md.timestamp(), started, System.currentTimeMillis());
    }

    /** The log-end offset of one partition: the offset the next record will get. */
    public long endOffset(String topic, int partition, Duration timeout) throws Exception {
        TopicPartition tp = new TopicPartition(topic, partition);
        ListOffsetsResult r = admin().listOffsets(Map.of(tp, OffsetSpec.latest()));
        return r.partitionResult(tp).get(timeout.toMillis(), TimeUnit.MILLISECONDS).offset();
    }

    /** One record read back from Kafka. */
    public static final class Rec {
        public int partition;
        public long offset;
        public long timestampMs;
        public String key;
        public String value;
    }

    /**
     * One long-lived reader (no group, no commits), reused by every {@link #read} call: creating a
     * consumer per read costs a metadata round trip and connection setup each time. It is created
     * lazily and discarded after any error, so a broker outage only fails the current read.
     */
    private final Object readLock = new Object();
    private KafkaConsumer<String, String> reader;

    private KafkaConsumer<String, String> reader() {
        if (reader == null) {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            p.put(ConsumerConfig.CLIENT_ID_CONFIG, "demo-app-reader");
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
            p.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 8_000);
            p.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 8_000);
            p.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 2_000);
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            reader = new KafkaConsumer<>(p);
        }
        return reader;
    }

    private void discardReader() {
        if (reader != null) {
            try {
                reader.close(Duration.ofSeconds(1));
            } catch (Exception ignored) {
                // already broken
            }
            reader = null;
        }
    }

    /**
     * Reads offsets [start, end) of one partition. Returns at most {@code max} records; fails if
     * the range cannot be read in time.
     */
    public List<Rec> read(String topic, int partition, long start, long end, int max, Duration timeout) throws Exception {
        List<Rec> out = new ArrayList<>();
        if (end <= start) {
            return out;
        }
        TopicPartition tp = new TopicPartition(topic, partition);
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        synchronized (readLock) {
            try {
                KafkaConsumer<String, String> c = reader();
                c.assign(List.of(tp));
                c.seek(tp, start);
                long next = start;
                while (next < end && out.size() < max) {
                    long left = deadline - System.currentTimeMillis();
                    if (left <= 0) {
                        throw new IllegalStateException("could not read " + topic + "-" + partition + " offsets [" + start
                                + ", " + end + ") within " + timeout.toSeconds() + " s (stopped at " + next + ")");
                    }
                    for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(Math.min(left, 500)))) {
                        if (r.offset() >= end || out.size() >= max) {
                            break;
                        }
                        Rec x = new Rec();
                        x.partition = r.partition();
                        x.offset = r.offset();
                        x.timestampMs = r.timestamp();
                        x.key = r.key();
                        x.value = r.value();
                        out.add(x);
                    }
                    next = c.position(tp);
                }
                c.assign(List.of()); // drop prefetched data; the next read seeks explicitly
            } catch (Exception e) {
                discardReader();
                throw e;
            }
        }
        return out;
    }

    @Override
    public synchronized void close() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        if (admin != null) {
            admin.close(Duration.ofSeconds(5));
        }
        synchronized (readLock) {
            discardReader();
        }
    }
}
