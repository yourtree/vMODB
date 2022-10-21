package dk.ku.di.dms.vms.modb.definition;

import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.SecondaryIndex;

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

    /**
     * Why foreign keys is on table and not on {@link Schema}?
     * (i) To avoid circular dependence schema <-> table.
     *      This way Schema does not know about the details of a Table
     *      (e.g., the primary index structure, the table name, the secondary indexes, etc).
     * (ii) The planner needs the foreign keys in order to define an optimal plan.
     *      As the table carries the PK, the table holds references to other tables.
     * (iii) Foreign key necessarily require coordination across different indexes that are maintaining records.
     *       In other words, foreign key maintenance is a concurrency control behavior.
     * /
     * The array int[] are the column positions, ordered, that form the foreign key (i.e., refer to the other table).
     * Who I am pointing to? The referenced or parent table.
     */
    private final Map<PrimaryIndex, int[]> foreignKeys;

    // Who is pointing to me? Who am I parenting?
    // private final List<ConsistentIndex> children;

    // all tables must have a pk. besides, used for fast path on planner
    private final PrimaryIndex primaryIndex;

    // Other indexes, hashed by the column set in order of the schema
    // logical key - column list in order that appear in the schema
    // physical key - column list in order of index definition
    public Map<IIndexKey, SecondaryIndex> secondaryIndexMap;

    public Table(String name, Schema schema, PrimaryIndex primaryIndex, Map<PrimaryIndex, int[]> foreignKeys){
        this.name = name;
        this.schema = schema;
        this.hashCode = name.hashCode();
        this.secondaryIndexMap = new HashMap<>();
        this.primaryIndex = primaryIndex;
        this.foreignKeys = foreignKeys;
    }

    public Table(String name, Schema schema, PrimaryIndex primaryIndex){
        this.name = name;
        this.schema = schema;
        this.hashCode = name.hashCode();
        this.secondaryIndexMap = new HashMap<>();
        this.primaryIndex = primaryIndex;
        this.foreignKeys = Collections.emptyMap();
    }

    @Override
    public int hashCode(){
        return this.hashCode;
    }

    public Schema getSchema(){
        return this.schema;
    }

    public String getName(){
        return this.name;
    }

    public ReadWriteIndex<IKey> underlyingPrimaryKeyIndex(){
        return this.primaryIndex.underlyingIndex();
    }

    public PrimaryIndex primaryKeyIndex(){
        return this.primaryIndex;
    }

    public Map<PrimaryIndex, int[]> foreignKeys(){
        return this.foreignKeys;
    }

}
