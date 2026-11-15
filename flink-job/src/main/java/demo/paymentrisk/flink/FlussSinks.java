package demo.paymentrisk.flink;

import demo.paymentrisk.common.TableSchemas.Column;
import demo.paymentrisk.common.TableSchemas.TableSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the Row types and INSERT statements that hand DataStream rows to the official Fluss
 * table connector. Column names and order come from {@link TableSpec}, the same contract the
 * bootstrap uses to create the tables.
 */
public final class FlussSinks {
    public static final String CATALOG = "fluss_catalog";

    private FlussSinks() {}

    public static RowTypeInfo rowType(TableSpec spec) {
        TypeInformation<?>[] types = new TypeInformation<?>[spec.columns.size()];
        String[] names = new String[spec.columns.size()];
        for (int i = 0; i < types.length; i++) {
            Column c = spec.columns.get(i);
            names[i] = c.name;
            switch (c.type) {
                case STRING: types[i] = Types.STRING; break;
                case INT: types[i] = Types.INT; break;
                case BOOLEAN: types[i] = Types.BOOLEAN; break;
                case BIGINT:
                case TIMESTAMP_LTZ_3: types[i] = Types.LONG; break; // timestamps: epoch millis
                default: throw new IllegalArgumentException("unsupported " + c.type);
            }
        }
        return new RowTypeInfo(types, names);
    }

    public static String viewName(TableSpec spec) {
        return "v_" + spec.name;
    }

    public static String createCatalogSql(String flussBootstrap) {
        return "CREATE CATALOG " + CATALOG + " WITH ('type' = 'fluss', 'bootstrap.servers' = '" + flussBootstrap + "')";
    }

    /** {@code INSERT INTO fluss_catalog.db.table (cols) SELECT ... FROM v_table} with timestamp conversion. */
    public static String insertSql(String database, TableSpec spec) {
        List<String> cols = new ArrayList<>();
        List<String> exprs = new ArrayList<>();
        for (Column c : spec.columns) {
            String q = "`" + c.name + "`";
            cols.add(q);
            exprs.add(c.type == demo.paymentrisk.common.TableSchemas.ColumnType.TIMESTAMP_LTZ_3
                    ? "TO_TIMESTAMP_LTZ(" + q + ", 3) AS " + q
                    : q);
        }
        return "INSERT INTO `" + CATALOG + "`.`" + database + "`.`" + spec.name + "` (" + String.join(", ", cols) + ")\n"
                + "SELECT " + String.join(", ", exprs) + "\nFROM `" + viewName(spec) + "`";
    }
}
