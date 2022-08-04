package dk.ku.di.dms.vms.modb.index.onheap;

import dk.ku.di.dms.vms.modb.index.BufferContext;
import dk.ku.di.dms.vms.modb.schema.key.IKey;
import dk.ku.di.dms.vms.modb.table.Table;
import jdk.incubator.foreign.MemorySegment;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Base implementation of an index
 * @param <K> extends {@link IKey}
 *
 * https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/core/memory/MemorySegment.java
 * https://stackoverflow.com/questions/24026918/java-nio-bytebuffer-allocatedirect-size-limit-over-the-int
 */
public abstract class AbstractIndex<K> {

    protected static final int BUCKET_SIZE = Integer.MAX_VALUE - 8;

    private final IndexKeyTypeEnum keyType;

    private final int[] columns;

    private final int hashCode;

    // respective table of this index
    protected final Table table;

    protected final BufferContext bufferContext;

    public AbstractIndex(BufferContext bufferContext, Table table, int... columnsIndex) {
        this.bufferContext = bufferContext;
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

    public abstract void insert(K key, ByteBuffer record);

    public abstract void update(K key, ByteBuffer record);

    public abstract void delete(K key);

    public abstract ByteBuffer retrieve(K key);

    // public abstract Collection<Row> retrieveCollection(K key);

    public abstract void retrieve(K key, ByteBuffer target);

    public abstract int size();

    // public abstract Collection<Row> rows();

    /** information used by the planner to decide for the appropriate operator */
    public abstract IndexDataStructureEnum getType();

    public IndexKeyTypeEnum getIndexKeyType(){
        return this.keyType;
    }

    public Table getTable(){
        return this.table;
    }

    public int[] getColumns(){
        return this.columns;
    }

}
