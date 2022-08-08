package dk.ku.di.dms.vms.modb.index.onheap;

import dk.ku.di.dms.vms.modb.storage.BufferContext;
import dk.ku.di.dms.vms.modb.table.Table;
import dk.ku.di.dms.vms.modb.schema.key.IKey;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;

import static dk.ku.di.dms.vms.modb.schema.Header.inactive;

/**
 * Unique hash indexes are used primarily for primary keys
 * But can also be used for secondary indexes
 *
 * In the future:
 *  - use UNSAFE for faster operations
 *  - pending writes must share the same buffer, so the operation can be faster (it is a bulk)
 *
 * Read @link{https://stackoverflow.com/questions/20824686/what-is-difference-between-primary-index-and-secondary-index-exactly}
 */
public class UniqueHashIndex extends AbstractIndex<IKey> {

    private static final Unsafe UNSAFE = Unsafe.getUnsafe();

    private volatile int size;

    public UniqueHashIndex(BufferContext bufferContext, Table table, int... columnsIndex){
        super(bufferContext, table, columnsIndex);
        this.size = 0;
    }

    // 1 - get (overall) position in the bytebuffer
    private int getLogicalPosition(IKey key){
        // https://algs4.cs.princeton.edu/34hash/
        return (key.hashCode() & 0x7fffffff) % bufferContext.capacity;
    }

    // 2 - get bucket from the set of bytebuffers
    private int getBucket(int logicalPosition){
        return (logicalPosition / bufferContext.buffers.length) - 1;
    }

    // 3 - calculate relative position in the buffer
    private int getPhysicalPosition(int logicalPosition){
        // need the record size from table
        return ( table.getSchema().getRecordSize() * logicalPosition ) / BUCKET_SIZE;
    }

    @Override
    public void insert(IKey key, ByteBuffer row) {
        update(key, row);
        this.size++;
    }

    /**
     * The update can be possibly optimized for updating only the fields required
     * instead of the whole record
     */
    @Override
    public void update(IKey key, ByteBuffer row) {

        int logicalPos = getLogicalPosition(key);
        int bucket = getBucket(logicalPos);
        int physicalPos = getPhysicalPosition(logicalPos);

        // 4 - copy contents
        // consider the active flag is already set by the entity parser
        // bufferContext.buffers[bucket].put(physicalPos, Header.active);
        bufferContext.buffers[bucket].position(physicalPos);
        bufferContext.buffers[bucket].put(row);

    }

    @Override
    public void delete(IKey key) {
        int logicalPos = getLogicalPosition(key);
        int bucket = getBucket(logicalPos);
        int physicalPos = getPhysicalPosition(logicalPos);
        bufferContext.buffers[bucket].position(physicalPos);
        bufferContext.buffers[bucket].put(inactive);
        this.size--;
    }

    @Override
    public ByteBuffer retrieve(IKey key) {
        int logicalPos = getLogicalPosition(key);
        int bucket = getBucket(logicalPos);
        int physicalPos = getPhysicalPosition(logicalPos);
        return bufferContext.buffers[bucket].slice(physicalPos, table.getSchema().getRecordSize() );
    }

    /**
     * Contents are copied into the target buffer
     */
    @Override
    public void retrieve(IKey key, ByteBuffer destBase) {
        int logicalPos = getLogicalPosition(key);
        int bucket = getBucket(logicalPos);
        int srcOffset = getPhysicalPosition(logicalPos);
        UNSAFE.copyMemory(bufferContext.buffers[bucket], srcOffset, destBase, 0, table.getSchema().getRecordSize());
    }

    /**
     * Check whether the record is active (if exists)
     */
    @Override
    public boolean exists(IKey key){
        int logicalPos = getLogicalPosition(key);
        int bucket = getBucket(logicalPos);
        int physicalPos = getPhysicalPosition(logicalPos);
        bufferContext.buffers[bucket].position(physicalPos);
        return bufferContext.buffers[bucket].get() != inactive;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.HASH;
    }

}
