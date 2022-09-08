package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;
import dk.ku.di.dms.vms.modb.definition.key.IKey;

import static dk.ku.di.dms.vms.modb.definition.Header.inactive;

/**
 * This index does not support growing number of keys
 * Could deal with collisions by having a linked list
 */
public class UniqueHashIndex extends AbstractIndex<IKey> {

    private final RecordBufferContext recordBufferContext;

    public UniqueHashIndex(RecordBufferContext recordBufferContext, Schema schema, int... columnsIndex){
        super(schema, columnsIndex);
        this.recordBufferContext = recordBufferContext;
    }

    /**
     * https://algs4.cs.princeton.edu/34hash/
     * "The code masks off the sign bit (to turn the 32-bit integer into a 31-bit non-negative integer)
     * and then computing the remainder when dividing by M, as in modular hashing."
     */
    private long getPosition(int key){
        int logicalPosition = (key & 0x7fffffff) % recordBufferContext.capacity;
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
        long pos = getPosition(key.hashCode());
        UNSAFE.copyMemory(null, srcAddress, null, pos, recordBufferContext.recordSize);
    }

    @Override
    public void delete(IKey key) {
        long pos = getPosition(key.hashCode());
        UNSAFE.putBoolean(null, pos, inactive);
        // this.size--;
    }

    public long retrieve(IKey key) {
        return getPosition(key.hashCode());
    }

//    public long retrieve(int key) {
//        return getPosition(key);
//    }

    /**
     * Check whether the record is active (if exists)
     */
    @Override
    public boolean exists(IKey key){
        long pos = getPosition(key.hashCode());
        return UNSAFE.getBoolean(null, pos);
    }

//    public boolean exists(int key){
//        long pos = getPosition(key);
//        return UNSAFE.getBoolean(null, pos);
//    }

    public boolean exists(long address){
        return UNSAFE.getBoolean(null, address);
    }

    @Override
    public int size() {
        return this.recordBufferContext.size;
    }

    @Override
    public IRecordIterator iterator() {
        return new RecordIterator(this.recordBufferContext.address, schema.getRecordSize(),
                this.recordBufferContext.capacity);
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.UNIQUE;
    }

    @Override
    public UniqueHashIndex asUniqueHashIndex(){
        return this;
    }

}
