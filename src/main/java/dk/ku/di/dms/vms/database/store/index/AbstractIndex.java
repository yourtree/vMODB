package dk.ku.di.dms.vms.database.store.index;

import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public abstract class AbstractIndex<K> {

    private final IndexTypeEnum type;
    private final int hashCode;

    // respective table of this index
    private final Table table;

    public AbstractIndex(final Table table, final int... columnsIndex) {
        this.table = table;
        if(columnsIndex.length == 1) {
            this.hashCode = columnsIndex[0];
            this.type = IndexTypeEnum.SINGLE;
        } else {
            this.hashCode = Arrays.hashCode(columnsIndex);
            this.type = IndexTypeEnum.COMPOSITE;
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

    public abstract boolean retrieve(K key, Row outputRow);

    public abstract int size();

    public abstract Collection<Row> rows();

    /** information used by the planner to decide for the appropriate operator */
    public abstract IndexDataStructureEnum getType();

    public Table getTable(){
        return this.table;
    }

    public abstract Set<Map.Entry<K,Row>> entrySet();

}
