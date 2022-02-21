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

    // at first, I am considering the table name is immutable. the hash code is cached to uniquely identify the table in java maps
    private final int hashCode;

    protected final String name;

    protected final Schema schema;

    protected AbstractIndex<IKey> primaryIndex;

    // Hashed by the column set
    protected Map<IKey, AbstractIndex<IKey>> secondaryIndexes;

    /** System-defined index to iterate over the rows.
     *  ATTENTION! Only used in case no primary index has been defined
     *   and no secondary index can be used in the query
     */
    protected AbstractIndex<IKey> internalIndex;

    public abstract int size();

    /** no primary index */
    public abstract boolean upsert(Row row) throws Exception;

    public abstract boolean upsert(IKey key, Row row);

    public abstract boolean delete(IKey key);

    public abstract Row retrieve(IKey key);

    public Table(final String name, final Schema schema) {
        this.name = name;
        this.schema = schema;
        this.hashCode = name.hashCode();
    }

    @Override
    public int hashCode(){
        return this.hashCode;
    }

    public Schema getSchema(){
        return schema;
    }

    public String getName(){
        return this.name;
    }

    public boolean hasPrimaryKey(){
        return primaryIndex != null;
    }

    public AbstractIndex<IKey> getPrimaryIndex(){
        return primaryIndex;
    }

    public AbstractIndex<IKey> getSecondaryIndexForColumnSetHash( final IKey key ){
        return secondaryIndexes.getOrDefault( key, null );
    }

    public AbstractIndex<IKey> getInternalIndex(){
        return internalIndex;
    }

    public Collection<AbstractIndex<IKey>> getSecondaryIndexes() {
        return secondaryIndexes.values();
    }
}
