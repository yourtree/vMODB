package dk.ku.di.dms.vms.modb.store.index;

import dk.ku.di.dms.vms.modb.store.row.Row;
import dk.ku.di.dms.vms.modb.store.table.Table;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public abstract class AbstractIndex<K> {

    private final IndexKeyTypeEnum keyType;

    private final int[] columns;

    private final int hashCode;

    // respective table of this index
    private final Table table;

    public AbstractIndex(final Table table, final int... columnsIndex) {
        this.table = table;
        this.columns = columnsIndex;
        if(columnsIndex.length == 1) {
            this.hashCode = columnsIndex[0];
            this.keyType = IndexKeyTypeEnum.SIMPLE;
        } else {
            this.hashCode = Arrays.hashCode(columnsIndex);
            this.keyType = IndexKeyTypeEnum.COMPOSITE;
        }
    }

    // use bitwise comparison to find whether a given index exists for such columns
    // https://stackoverflow.com/questions/8504288/java-bitwise-comparison-of-a-byte/8504393
    public int hashCode(){
        return this.hashCode;
    }

    public abstract boolean upsert(K key, Row row);

    public abstract boolean delete(K key);

    public abstract Row retrieve(K key);

    public abstract Collection<Row> retrieveCollection(K key);

    public abstract boolean retrieve(K key, Row outputRow);

    public abstract int size();

    public abstract Collection<Row> rows();

    /** information used by the planner to decide for the appropriate operator */
    public abstract IndexDataStructureEnum getType();

    public IndexKeyTypeEnum getIndexKeyType(){
        return this.keyType;
    }

    public Table getTable(){
        return this.table;
    }

    public abstract Set<Map.Entry<K,Row>> entrySet() throws UnsupportedIndexOperationException;

    public int[] getColumns(){
        return this.columns;
    }

}
