package demo.paymentrisk.app.infra;

import demo.paymentrisk.common.TableSchemas;
import demo.paymentrisk.common.TableSchemas.Column;
import demo.paymentrisk.common.TableSchemas.TableSpec;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Native Fluss access through the Java client: Admin for DDL/health, and primary-key
 * {@link Lookuper}s for reads. Nothing here reads from Kafka or from a local cache.
 */
public final class FlussGateway implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FlussGateway.class);
    public static final String READ_METHOD =
            "Fluss Java client: Table.newLookup().createLookuper().lookup(primary key)";

    private final String bootstrap;
    private final String database;
    private Connection connection;
    private Admin admin;
    private final Map<String, Table> tables = new HashMap<>();
    private final Map<String, Lookuper> lookupers = new HashMap<>();
    private int consecutiveFailures;

    public FlussGateway(String bootstrap, String database) {
        this.bootstrap = bootstrap;
        this.database = database;
    }

    public String database() {
        return database;
    }

    private synchronized Connection connection() {
        if (connection == null) {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        }
        return connection;
    }

    private synchronized Admin admin() {
        connection();
        return admin;
    }

    /** Drops the connection after repeated failures so the next call reconnects cleanly. */
    public synchronized void reset() {
        lookupers.clear();
        for (Table t : tables.values()) {
            try {
                t.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        tables.clear();
        if (admin != null) {
            try {
                admin.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        admin = null;
        connection = null;
        consecutiveFailures = 0;
    }

    public TablePath path(TableSpec spec) {
        return TablePath.of(database, spec.name);
    }

    // ------------------------------------------------------------------ cluster / DDL

    public static final class Nodes {
        public int coordinators;
        public int tabletServers;
        public List<String> endpoints = new ArrayList<>();
    }

    public Nodes serverNodes(Duration timeout) throws Exception {
        List<ServerNode> nodes = admin().getServerNodes().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        Nodes n = new Nodes();
        for (ServerNode s : nodes) {
            if (s.serverType() == ServerType.COORDINATOR) {
                n.coordinators++;
            } else if (s.serverType() == ServerType.TABLET_SERVER) {
                n.tabletServers++;
            }
            n.endpoints.add(s.serverType() + "#" + s.id() + "@" + s.host() + ":" + s.port());
        }
        return n;
    }

    public void createDatabase(Duration timeout) throws Exception {
        admin().createDatabase(database, DatabaseDescriptor.EMPTY, true).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public boolean tableExists(TableSpec spec, Duration timeout) throws Exception {
        return admin().tableExists(path(spec)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** @return true if created now. */
    public boolean createTable(TableSpec spec, Duration timeout) throws Exception {
        if (tableExists(spec, timeout)) {
            return false;
        }
        Schema.Builder sb = Schema.newBuilder();
        for (Column c : spec.columns) {
            sb.column(c.name, flussType(c.type)).withComment(c.comment);
        }
        sb.primaryKey(spec.primaryKey);
        TableDescriptor td = TableDescriptor.builder()
                .schema(sb.build())
                .distributedBy(1, spec.primaryKey) // one bucket: single TabletServer demo
                .comment(spec.comment)
                .build();
        admin().createTable(path(spec), td, true).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        return true;
    }

    /** Differences between the existing table and the contract (empty if they match). */
    public List<String> schemaDifferences(TableSpec spec, Duration timeout) throws Exception {
        TableInfo info = admin().getTableInfo(path(spec)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        List<String> diffs = new ArrayList<>();
        List<String> actual = info.getRowType().getFieldNames();
        if (!actual.equals(spec.columnNames())) {
            diffs.add("columns differ: expected " + spec.columnNames() + " but found " + actual);
        }
        if (!info.getPrimaryKeys().equals(spec.primaryKey)) {
            diffs.add("primary key differs: expected " + spec.primaryKey + " but found " + info.getPrimaryKeys());
        }
        return diffs;
    }

    private static DataType flussType(TableSchemas.ColumnType t) {
        switch (t) {
            case STRING: return DataTypes.STRING();
            case INT: return DataTypes.INT();
            case BIGINT: return DataTypes.BIGINT();
            case BOOLEAN: return DataTypes.BOOLEAN();
            case TIMESTAMP_LTZ_3: return DataTypes.TIMESTAMP_LTZ(3);
            default: throw new IllegalArgumentException(String.valueOf(t));
        }
    }

    // ------------------------------------------------------------------ lookups

    /** The outcome of one native primary-key lookup, success or failure, with timing. */
    @com.fasterxml.jackson.databind.annotation.JsonNaming(com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Observation {
        public String table;
        public Map<String, String> key;
        public boolean ok;
        public boolean found;
        public Map<String, Object> row;
        /** When the lookup was issued. */
        public String queriedAt;
        public long queriedAtMs;
        /** When the response arrived: the moment the row was actually observed. */
        public String respondedAt;
        public long respondedAtMs;
        public double durationMs;
        public String error;
        public String source = READ_METHOD;
    }

    private synchronized Lookuper lookuper(TableSpec spec) {
        Lookuper l = lookupers.get(spec.name);
        if (l == null) {
            Table t = connection().getTable(path(spec));
            tables.put(spec.name, t);
            l = t.newLookup().createLookuper();
            lookupers.put(spec.name, l);
        }
        return l;
    }

    private synchronized RowType rowType(TableSpec spec) {
        Table t = tables.get(spec.name);
        return t == null ? null : t.getTableInfo().getRowType();
    }

    /** Looks up one row by its full primary key (values in primary-key order). */
    public Observation lookup(TableSpec spec, List<String> keyValues, Duration timeout) {
        Observation o = new Observation();
        o.table = database + "." + spec.name;
        o.key = new LinkedHashMap<>();
        for (int i = 0; i < spec.primaryKey.size(); i++) {
            o.key.put(spec.primaryKey.get(i), keyValues.get(i));
        }
        long start = System.nanoTime();
        o.queriedAtMs = System.currentTimeMillis();
        o.queriedAt = Instant.ofEpochMilli(o.queriedAtMs).toString();
        try {
            Object[] k = new Object[keyValues.size()];
            for (int i = 0; i < k.length; i++) {
                k[i] = BinaryString.fromString(keyValues.get(i));
            }
            LookupResult r = lookuper(spec).lookup(GenericRow.of(k)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            o.durationMs = (System.nanoTime() - start) / 1_000_000.0;
            o.respondedAtMs = System.currentTimeMillis();
            o.respondedAt = Instant.ofEpochMilli(o.respondedAtMs).toString();
            o.ok = true;
            InternalRow row = r.getRowList().isEmpty() ? null : r.getRowList().get(0);
            o.found = row != null;
            o.row = row == null ? null : toMap(row, rowType(spec));
            synchronized (this) {
                consecutiveFailures = 0;
            }
        } catch (Exception e) {
            o.durationMs = (System.nanoTime() - start) / 1_000_000.0;
            o.respondedAtMs = System.currentTimeMillis();
            o.respondedAt = Instant.ofEpochMilli(o.respondedAtMs).toString();
            o.ok = false;
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            o.error = root.getClass().getSimpleName() + ": " + String.valueOf(root.getMessage());
            boolean reset;
            synchronized (this) {
                reset = ++consecutiveFailures >= 5;
            }
            if (reset) {
                LOG.warn("Fluss lookups failing repeatedly ({}); reconnecting", o.error);
                reset();
            }
        }
        return o;
    }

    private static Map<String, Object> toMap(InternalRow row, RowType type) {
        Map<String, Object> m = new LinkedHashMap<>();
        List<DataField> fields = type.getFields();
        InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(type);
        for (int i = 0; i < fields.size(); i++) {
            Object v = getters[i].getFieldOrNull(row);
            if (v instanceof BinaryString) {
                v = v.toString();
            } else if (v instanceof TimestampLtz) {
                v = ((TimestampLtz) v).toInstant().toString();
            }
            m.put(fields.get(i).getName(), v);
        }
        return m;
    }

    @Override
    public void close() {
        reset();
    }
}
