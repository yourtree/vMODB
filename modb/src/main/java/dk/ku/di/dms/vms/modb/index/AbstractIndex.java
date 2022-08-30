package dk.ku.di.dms.vms.modb.index;

import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.common.meta.MemoryUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.Table;
import sun.misc.Unsafe;

import java.util.*;

/**
 * Base implementation of an index
 * @param <K> extends {@link IKey}
 *
 * https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/core/memory/MemorySegment.java
 * https://stackoverflow.com/questions/24026918/java-nio-bytebuffer-allocatedirect-size-limit-over-the-int
 */
public abstract class AbstractIndex<K> {

    protected static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    protected final int[] columns;

    private final int hashCode;

    // respective table of this index
    protected final Table table;

    public AbstractIndex(Table table, int... columnsIndex) {
        this.table = table;
        this.columns = columnsIndex;
        if(columnsIndex.length == 1) {
            this.hashCode = columnsIndex[0];
        } else {
            this.hashCode = Arrays.hashCode(columnsIndex);
        }
    }

    // use bitwise comparison to find whether a given index exists for such columns
    // https://stackoverflow.com/questions/8504288/java-bitwise-comparison-of-a-byte/8504393
    public int hashCode(){
        return this.hashCode;
    }

    public abstract void insert(K key, long srcAddress);

    public abstract void update(K key, long srcAddress);

    public abstract void delete(K key);

    public abstract boolean exists(K key);

    public Table getTable(){
        return this.table;
    }

    public int[] getColumns(){
        return this.columns;
    }

    public abstract int size();

    /** information used by the planner to decide for the appropriate operator */
    public abstract IndexTypeEnum getType();

    public UniqueHashIndex asUniqueHashIndex(){
        throw new IllegalStateException("Concrete index does not override this method.");
    }

    public NonUniqueHashIndex asNonUniqueHashIndex(){
        throw new IllegalStateException("Concrete index does not override this method.");
    }

}
