package dk.ku.di.dms.vms.database.store.table;

import dk.ku.di.dms.vms.database.store.row.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.index.IIndex;

import java.util.Collection;
import java.util.Map;

public abstract class Table {

    protected final String name;

    protected final Schema schema;

    protected IIndex primaryIndex;

    protected Map<IKey, IIndex> secondaryIndexes;

    public abstract int size();

    /** no primary index */
    public abstract boolean upsert(Row row) throws Exception;

    public abstract boolean upsert(IKey key, Row row);

    public abstract boolean delete(IKey key);

    public abstract Row retrieve(IKey key);

    public abstract Collection<Row> rows();

    public Table(String name, Schema schema) {
        this.name = name;
        this.schema = schema;
    }

    public Schema getSchema(){
        return schema;
    }

    public boolean hasPrimaryKey(){
        return primaryIndex != null;
    }

    public String getName(){
        return this.name;
    }
}
