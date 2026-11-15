package demo.paymentrisk.common;

import demo.paymentrisk.common.TableSchemas.Column;
import demo.paymentrisk.common.TableSchemas.TableSpec;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A row destined for one Fluss table, addressed by column name and type-checked against the
 * {@link TableSpec}. Timestamp columns hold epoch milliseconds ({@link Long}).
 */
public final class OutputRow {
    private final TableSpec table;
    private final Object[] values;

    private OutputRow(TableSpec table) {
        this.table = table;
        this.values = new Object[table.columns.size()];
    }

    public static OutputRow of(TableSpec table) {
        return new OutputRow(table);
    }

    public OutputRow set(String column, Object value) {
        int i = table.indexOf(column);
        if (i < 0) {
            throw new IllegalArgumentException(table.name + " has no column " + column);
        }
        if (value != null) {
            Column c = table.columns.get(i);
            Class<?> expected;
            switch (c.type) {
                case STRING: expected = String.class; break;
                case INT: expected = Integer.class; break;
                case BOOLEAN: expected = Boolean.class; break;
                default: expected = Long.class; break;
            }
            if (!expected.isInstance(value)) {
                throw new IllegalArgumentException(table.name + "." + column + " expects " + expected.getSimpleName()
                        + " but got " + value.getClass().getSimpleName());
            }
        }
        values[i] = value;
        return this;
    }

    public Object get(String column) {
        int i = table.indexOf(column);
        if (i < 0) {
            throw new IllegalArgumentException(table.name + " has no column " + column);
        }
        return values[i];
    }

    public TableSpec table() {
        return table;
    }

    /** Values in column order, for conversion into a Flink {@code Row}. */
    public Object[] values() {
        return values.clone();
    }

    public Map<String, Object> asMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i++) {
            m.put(table.columns.get(i).name, values[i]);
        }
        return m;
    }

    @Override
    public String toString() {
        return table.name + asMap();
    }
}
