package dk.ku.di.dms.vms.modb.index;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import sun.misc.Unsafe;

import java.util.*;

/**
 * Base implementation of an index
 * @param <K> extends {@link IKey}
 *
 * https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/core/memory/MemorySegment.java
 * https://stackoverflow.com/questions/24026918/java-nio-bytebuffer-allocatedirect-size-limit-over-the-int
 */
public abstract class AbstractIndex<K> implements ReadWriteIndex<K> {

    protected static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    protected final int[] columns;

    // to speed up queries, so the filters can be build on flight
    protected final HashSet<Integer> columnsHash;

    private final IIndexKey key;

    private final int hashCode;

    // respective table of this index
    protected final Schema schema;

    public AbstractIndex(Schema schema, int... columnsIndex) {
        this.schema = schema;
        this.columns = columnsIndex;
        if(columnsIndex.length == 1) {
            this.hashCode = columnsIndex[0];
        } else {
            this.hashCode = Arrays.hashCode(columnsIndex);
        }
        this.key = SimpleKey.of(this.hashCode);
        this.columnsHash = new HashSet<>(columns.length);
        for(int i : columnsIndex) this.columnsHash.add(i);
    }

    // use bitwise comparison to find whether a given index exists for such columns
    // https://stackoverflow.com/questions/8504288/java-bitwise-comparison-of-a-byte/8504393
    public int hashCode(){
        return this.hashCode;
    }

    public int[] columns(){
        return this.columns;
    }

    public HashSet<Integer> columnsHash() {
        return columnsHash;
    }

    @Override
    public IIndexKey key(){
        return this.key;
    }

    public abstract int size();

    public Schema schema(){
        return this.schema;
    }

}
