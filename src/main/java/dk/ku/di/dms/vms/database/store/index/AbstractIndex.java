package dk.ku.di.dms.vms.database.store.index;

import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.Arrays;
import java.util.Collection;

public abstract class AbstractIndex<K> {

    // I need this to figure out whether this index is applied to a set of columns
    private final int[] columnsIndex;

    public AbstractIndex(final int... columnsIndex) {
        this.columnsIndex = columnsIndex;
    }

    // use bitwise comparison to find whether a given index exists for such columns
    // https://stackoverflow.com/questions/8504288/java-bitwise-comparison-of-a-byte/8504393
    public int hashCode(){
        return Arrays.hashCode(columnsIndex);
    }

    public abstract boolean upsert(K key, Row row);

    public abstract boolean delete(K key);

    public abstract Row retrieve(K key);

    public abstract boolean retrieve(K key, Row outputRow);

    public abstract int size();

    public abstract Collection<Row> rows();

    /** information used by the planner to decide for the appropriate operator */
    public abstract IndexTypeEnum getType();

}
