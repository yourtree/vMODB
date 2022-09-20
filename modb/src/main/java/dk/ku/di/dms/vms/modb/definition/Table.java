package dk.ku.di.dms.vms.modb.definition;

import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;

import java.util.*;

/**
 * Basic building block
 * This class holds the metadata to other data structures that concern a table and its operations
 * In other words, it does not hold/store rows, since this is the task of an index
 */
public final class Table {

    // at first, I am considering the table name is immutable. the hash code is cached to uniquely identify the table in java maps
    public final int hashCode;

    public final String name;

    public final Schema schema;

    // to avoid circular dependence schema <-> table
    // the array int[] means the column indexes, ordered, that refer to the other table
    public final Map<Table, int[]> foreignKeysGroupedByTableMap;

    // all tables must have a pk. besides, used for fast path on planner
    public UniqueHashIndex primaryKeyIndex;

    // Other indexes, hashed by the column set in order of the schema. The IKey is indexed by the order of columns in the index
    // public Map<IIndexKey, Map<IKey, AbstractIndex<IKey>>> indexes;
    public Map<IIndexKey, AbstractIndex<IKey>> indexes;

    // just a cached list of the indexes map to avoid using iterators from the map when deciding for an index
    public List<AbstractIndex<IKey>> indexList;

    public Table(String name, Schema schema, Map<Table,int[]> foreignKeysGroupedByTableMap) {
        this.name = name;
        this.schema = schema;
        this.hashCode = name.hashCode();
        this.indexes = new HashMap<>();
        this.indexList = new ArrayList<>();
        this.foreignKeysGroupedByTableMap = foreignKeysGroupedByTableMap;
    }

    public Table(String name, Schema schema){
        this.name = name;
        this.schema = schema;
        this.hashCode = name.hashCode();
        this.indexes = new HashMap<>();
        this.indexList = new ArrayList<>();
        this.foreignKeysGroupedByTableMap = null;
    }

    public Table(String name, Schema schema, UniqueHashIndex primaryKeyIndex){
        this.name = name;
        this.schema = schema;
        this.hashCode = name.hashCode();
        this.indexes = new HashMap<>();
        this.indexList = new ArrayList<>();
        this.foreignKeysGroupedByTableMap = null;
        this.primaryKeyIndex = primaryKeyIndex;
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

    public AbstractIndex<IKey> primaryKeyIndex(){
        return primaryKeyIndex;
    }

//    public Map<IIndexKey, Map<IKey,AbstractIndex<IKey>>> getSecondaryIndexes(){
//        return this.indexes;
//    }

    public Map<IIndexKey, AbstractIndex<IKey>> getSecondaryIndexes(){
        return this.indexes;
    }

    public List<AbstractIndex<IKey>> getIndexes() {
        // return indexes.values().stream().flatMap(List::stream).collect(Collectors.toList());
        // to avoid resorting to stream() and java operators
        return indexList;
    }

    // logical key - column list in order that appear in the schema
    // physical key - column list in order of index definition
//    public void addIndex( final IIndexKey indexLogicalKey, final IKey indexPhysicalKey, AbstractIndex<IKey> index ){
//        Map<IKey,AbstractIndex<IKey>> indexMap = this.indexes.get(indexLogicalKey);
//
//        if( indexMap == null ){
//            indexMap = new HashMap<>();
//        }
//
//        indexMap.put( indexPhysicalKey, index );
//        this.indexes.putIfAbsent( indexLogicalKey, indexMap );
//        this.indexList.add( index );
//    }

    public Map<Table, int[]> getForeignKeysGroupedByTable(){
        return this.foreignKeysGroupedByTableMap;
    }

}
