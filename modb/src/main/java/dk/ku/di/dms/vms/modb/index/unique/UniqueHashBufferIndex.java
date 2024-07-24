package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteBufferIndex;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.unique.KeyRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.unique.RecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This index does not support growing number of keys
 * Could deal with collisions by having a linked list.
 * This index is oblivious to isolation level and relational constraints.
 */
public final class UniqueHashBufferIndex extends ReadWriteIndex<IKey> implements ReadWriteBufferIndex<IKey> {

    private final RecordBufferContext recordBufferContext;

    private final AtomicInteger size;

    private final long recordSize;

    public UniqueHashBufferIndex(RecordBufferContext recordBufferContext, Schema schema, int[] columnsIndex){
        super(schema, columnsIndex);
        this.recordBufferContext = recordBufferContext;
        this.recordSize = schema.getRecordSize();
        this.size = new AtomicInteger(0);
    }

    /**
     * <a href="https://algs4.cs.princeton.edu/34hash/">Why (key & 0x7fffffff)?</a>
     * "The code masks off the sign bit (to turn the 32-bit integer into a 31-bit non-negative integer)
     * and then computing the remainder when dividing by M, as in modular hashing."
     */
    private long getPosition(int key){
        long logicalPosition = (key & 0x7fffffff) % this.recordBufferContext.capacity;
        return this.recordBufferContext.address + ( this.recordSize * logicalPosition );
    }

    @Override
    public void insert(IKey key, long srcAddress) {
        long pos = this.getPosition(key.hashCode());
        if(UNSAFE.getByte(null, pos) == Header.ACTIVE_BYTE){
            System.out.println("Overwriting previously written record!");
        }
        UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
        UNSAFE.putInt(null, pos, key.hashCode());
        UNSAFE.copyMemory(null, srcAddress, null, pos + Schema.RECORD_HEADER, schema.getRecordSizeWithoutHeader());
        this.updateSize(1);
    }

    private void updateSize(int val){
        this.size.addAndGet(val);
    }

    /**
     * The update can be possibly optimized for updating only the fields required instead
     * of the whole record. This method assumes the srcAddress begins with header data
     */
    @Override
    public void update(IKey key, long srcAddress) {
        long pos = getPosition(key.hashCode());
        UNSAFE.copyMemory(null, srcAddress, null, pos, this.recordSize);
    }

    @Override
    public void update(IKey key, Object[] record){
        long pos = this.getPosition(key.hashCode());
        int maxColumns = this.schema().columnOffset().length;
        long currAddress = pos + Schema.RECORD_HEADER;
        for(int index = 0; index < maxColumns; index++) {
            DataType dt = this.schema().columnDataType(index);
            DataTypeUtils.callWriteFunction( currAddress, dt, record[index] );
            currAddress += dt.value;
        }
    }

    @Override
    public void insert(IKey key, Object[] record){
        try {
            long pos = this.getPosition(key.hashCode());
            if(UNSAFE.getByte(null, pos) == Header.ACTIVE_BYTE){
                System.out.println("Overwriting previously written record. Key: " + key+ " Hash: " + key.hashCode());
            }
            UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
            UNSAFE.putInt(null, pos + Header.SIZE, key.hashCode());
            int maxColumns = this.schema.columnOffset().length;
            long currAddress = pos + Schema.RECORD_HEADER;
            for (int index = 0; index < maxColumns; index++) {
                DataType dt = this.schema.columnDataType(index);
                // null constraint should be validated by the transaction manager
                if(record[index] != null) {
                    DataTypeUtils.callWriteFunction(currAddress, dt, record[index]);
                }
                currAddress += dt.value;
            }
            this.updateSize(1);
        } catch (Exception e ){
            throw new RuntimeException("Error inserting record: "+e.getMessage());
        }
    }

    @Override
    public void delete(IKey key) {
        long pos = this.getPosition(key.hashCode());
        UNSAFE.putByte(null, pos, Header.INACTIVE_BYTE);
        this.updateSize(-1);
    }

    @Override
    public long address(IKey key) {
        return this.getPosition(key.hashCode());
    }

    /**
     * Check whether the record is active (if exists)
     */
    @Override
    public boolean exists(IKey key){
        long pos = this.getPosition(key.hashCode());
        return UNSAFE.getByte(null, pos) == Header.ACTIVE_BYTE;
    }

    @Override
    public Object[] lookupByKey(IKey key){
        var pos = this.getPosition(key.hashCode());
        if(this.exists(pos))
            return this.readFromIndex(pos + Schema.RECORD_HEADER);
        return null;
    }

    @Override
    public boolean exists(long address){
        return UNSAFE.getByte(null, address) == Header.ACTIVE_BYTE;
    }

    @Override
    public int size() {
        return this.size.get();
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return new RecordIterator(this.recordBufferContext.address,
                this.schema.getRecordSize(), this.recordBufferContext.capacity);
    }

    @Override
    public IRecordIterator<IKey> iterator(IKey[] keys) {
        return new KeyRecordIterator(this, keys);
    }

    @Override
    public boolean checkCondition(IRecordIterator<IKey> iterator, FilterContext filterContext) {
        return false;
    }

    @Override
    public Object[] record(IRecordIterator<IKey> iterator) {
        return this.readFromIndex(iterator.address() + Schema.RECORD_HEADER);
    }

    @Override
    public Object[] record(IKey key) {
        return this.readFromIndex(this.getPosition(key.hashCode()) + Schema.RECORD_HEADER);
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.UNIQUE;
    }

    @Override
    public void flush() {
        this.recordBufferContext.force();
    }

}
