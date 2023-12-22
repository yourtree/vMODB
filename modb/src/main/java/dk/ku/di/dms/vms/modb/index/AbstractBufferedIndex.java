package dk.ku.di.dms.vms.modb.index;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteBufferIndex;
import jdk.internal.misc.Unsafe;

/**
 * Base implementation of an index
 * @param <K> extends {@link IKey}
 * Inspiration from:
 * <a href="https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/core/memory/MemorySegment.java">...</a>
 * <a href="https://stackoverflow.com/questions/24026918/java-nio-bytebuffer-allocatedirect-size-limit-over-the-int">...</a>
 */
public abstract class AbstractBufferedIndex<K> extends AbstractIndex<K> implements ReadWriteBufferIndex<K> {

    protected static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    public AbstractBufferedIndex(Schema schema, int... columnsIndex) {
        super(schema, columnsIndex);
    }
}
