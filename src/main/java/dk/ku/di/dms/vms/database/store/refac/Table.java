package dk.ku.di.dms.vms.database.store.refac;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public abstract class Table {

    // basically a map of column name to exact position in values
    private final Map<String,Integer> columnNameToPosMap;

    private final String name;

    private final Schema schema;

    public abstract int size();

    public abstract boolean upsert(IKey key, Row row);

    public abstract boolean delete(IKey key);

    public abstract Iterator<Row> iterator();

    public Table(Map<String, Integer> columnNameToIndexMap, String name, Schema schema) {
        this.columnNameToPosMap = columnNameToIndexMap;
        this.name = name;
        this.schema = schema;
    }
}
