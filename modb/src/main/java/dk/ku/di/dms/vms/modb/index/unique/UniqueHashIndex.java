package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.storage.RecordBufferContext;
import dk.ku.di.dms.vms.modb.table.Table;
import dk.ku.di.dms.vms.modb.schema.key.IKey;

import static dk.ku.di.dms.vms.modb.schema.Header.inactive;

/**
 *
 */
public class UniqueHashIndex extends AbstractIndex<IKey> {

    private final RecordBufferContext recordBufferContext;

    public UniqueHashIndex(RecordBufferContext recordBufferContext, Table table, int... columnsIndex){
        super(table, columnsIndex);
        this.recordBufferContext = recordBufferContext;
    }

    /**
     * https://algs4.cs.princeton.edu/34hash/
     * "The code masks off the sign bit (to turn the 32-bit integer into a 31-bit non-negative integer)
     * and then computing the remainder when dividing by M, as in modular hashing."
     */
    private long getPosition(IKey key){
        int logicalPosition = (key.hashCode() & 0x7fffffff) % recordBufferContext.capacity;
        return recordBufferContext.address + ( recordBufferContext.recordSize * logicalPosition );
    }

    @Override
    public void insert(IKey key, long srcAddress) {
        update(key, srcAddress);
        // this.size++; // this should only be set after commit, so we spread the overhead
    }

    /**
     * The update can be possibly optimized for updating only the fields required
     * instead of the whole record
     */
    @Override
    public void update(IKey key, long srcAddress) {
        long pos = getPosition(key);
        UNSAFE.copyMemory(null, srcAddress, null, pos, recordBufferContext.recordSize);
    }

    @Override
    public void delete(IKey key) {
        long pos = getPosition(key);
        UNSAFE.putByte(pos, inactive);
        // this.size--;
    }

    @Override
    public long retrieve(IKey key) {
        return getPosition(key);
    }

    /**
     * Check whether the record is active (if exists)
     */
    @Override
    public boolean exists(IKey key){
        long pos = getPosition(key);
        return UNSAFE.getByte(pos) != inactive;
    }

    @Override
    public int size() {
        return this.recordBufferContext.size;
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.UNIQUE;
    }

}
