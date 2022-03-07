package dk.ku.di.dms.vms.database.store.table;

import dk.ku.di.dms.vms.database.store.index.IIndexKey;
import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;

import java.util.*;

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

    // for fast path on planner
    protected AbstractIndex<IKey> primaryKeyIndex;

    // Other indexes, hashed by the column set in order of the schema. The IKey is indexed by the order of columns in the index
    protected Map<IIndexKey, Map<IKey,AbstractIndex<IKey>>> indexes;

    // just a cached list of the indexes map
    protected List<AbstractIndex<IKey>> indexList;

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
        this.indexes = new HashMap<>();
        this.indexList = new ArrayList<>();
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
        return primaryKeyIndex != null;
    }

    public AbstractIndex<IKey> getPrimaryKeyIndex(){
        return primaryKeyIndex;
    }

    // A key is formed by a column set ordered by the order specified in the schema definition
    public AbstractIndex<IKey> getIndexByKey(final IIndexKey key){
        if(indexes.containsKey( key )) {
            return indexes.get( key ).get( key );
        }
        return null;
    }

    public Collection<AbstractIndex<IKey>> getIndexesByIndexKey(final IIndexKey key) {
        return indexes.get( key ).values();
    }

    public List<AbstractIndex<IKey>> getIndexes() {
        // return indexes.values().stream().flatMap(List::stream).collect(Collectors.toList());
        // to avoid resorting to stream() and java operators
        return indexList;
    }

    // logical key - column list in order that appear in the schema
    // physical key - column list in order of index definition
    public void addIndex( final IIndexKey indexLogicalKey, final IKey indexPhysicalKey, AbstractIndex<IKey> index ){
        Map<IKey,AbstractIndex<IKey>> indexMap = this.indexes.get(indexLogicalKey);

        if( indexMap == null ){
            indexMap = new HashMap<>();
        }

        indexMap.put( indexPhysicalKey, index );
        this.indexes.putIfAbsent( indexLogicalKey, indexMap );
        this.indexList.add( index );
    }

}
