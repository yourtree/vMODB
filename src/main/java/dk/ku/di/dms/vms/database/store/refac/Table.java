package dk.ku.di.dms.vms.database.store.refac;

import dk.ku.di.dms.vms.database.store.index.AbstractIndex;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class Table {

    // basically a map of column name to exact position in values
    protected final Map<String,Integer> columnNameToPosMap;

    protected final String name;

    protected final Schema schema;

    protected AbstractIndex primaryIndex;

    protected List<AbstractIndex> secondaryIndexes;

    public abstract int size();

    /** no primary index */
    public abstract boolean upsert(Row row);

    public abstract boolean upsert(IKey key, Row row);

    public abstract boolean delete(IKey key);

    public abstract Iterator<Row> iterator();

    public abstract Collection<Row> rows();

    public Table(Map<String, Integer> columnNameToIndexMap, String name, Schema schema) {
        this.columnNameToPosMap = columnNameToIndexMap;
        this.name = name;
        this.schema = schema;
    }
}
