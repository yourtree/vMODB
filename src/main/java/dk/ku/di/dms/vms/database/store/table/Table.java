package dk.ku.di.dms.vms.database.store.table;

import dk.ku.di.dms.vms.database.store.row.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;

import java.util.Collection;
import java.util.Map;

/**
 * Basic building block
 * This class holds the metadata to other data structures that concern a table and its operations
 * In other words, it does not hold/store rows, since this is the task of an index
 */
public abstract class Table {

    protected final String name;

    protected final Schema schema;

    protected AbstractIndex primaryIndex;

    protected Map<IKey, AbstractIndex> secondaryIndexes;

    /** System-defined index to iterate over the rows.
     *  ATTENTION! Only used in case no primary index has been defined
     *   and no secondary index can be used in the query
     */
    protected AbstractIndex internalIndex;

    public abstract int size();

    /** no primary index */
    public abstract boolean upsert(Row row) throws Exception;

    public abstract boolean upsert(IKey key, Row row);

    public abstract boolean delete(IKey key);

    public abstract Row retrieve(IKey key);

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

    public AbstractIndex getPrimaryIndex(){
        return primaryIndex;
    }

    public String getName(){
        return this.name;
    }

    public Collection<AbstractIndex> getSecondaryIndexes(){
        return secondaryIndexes.values();
    }

    public AbstractIndex getInternalIndex(){
        return internalIndex;
    }

}
