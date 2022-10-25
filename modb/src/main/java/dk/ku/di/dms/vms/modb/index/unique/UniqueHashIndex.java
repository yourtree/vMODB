package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.unique.KeyRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.unique.RecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.modb.definition.Header.inactive;

/**
 * This index does not support growing number of keys
 * Could deal with collisions by having a linked list.
 * This index is oblivious to isolation level and data constraints.
 */
public final class UniqueHashIndex extends AbstractIndex<IKey> {

    private static final Logger logger = Logger.getLogger("UniqueHashIndex");

    private final RecordBufferContext recordBufferContext;

    private final Map<IKey, Object[]> cacheObjectStore;

    public UniqueHashIndex(RecordBufferContext recordBufferContext, Schema schema){
        super(schema, schema.getPrimaryKeyColumns());
        this.recordBufferContext = recordBufferContext;
        this.cacheObjectStore = new ConcurrentHashMap<>();
    }

    /**
     * Unique index for non-primary keys.
     * In other words, a constructor for a secondary index
     */
    public UniqueHashIndex(RecordBufferContext recordBufferContext, Schema schema, int... columnsIndex){
        super(schema, columnsIndex);
        this.recordBufferContext = recordBufferContext;
        this.cacheObjectStore = new ConcurrentHashMap<>();
    }

    /**
     * <a href="https://algs4.cs.princeton.edu/34hash/">Why (key & 0x7fffffff)?</a>
     * "The code masks off the sign bit (to turn the 32-bit integer into a 31-bit non-negative integer)
     * and then computing the remainder when dividing by M, as in modular hashing."
     */
    private long getPosition(int key){
        int logicalPosition = (key & 0x7fffffff) % recordBufferContext.capacity;
        return recordBufferContext.address + ( recordBufferContext.recordSize * logicalPosition );
    }

    @Override
    public void insert(IKey key, Object[] record){

        long pos = getPosition(key.hashCode());

        UNSAFE.putBoolean(null, pos, true);
        UNSAFE.putInt(null, pos, key.hashCode());

        int maxColumns = this.schema.columnOffset().length;
        long currAddress = pos + Header.SIZE + Integer.BYTES;

        for(int index = 0; index < maxColumns; index++) {

            DataType dt = this.schema.columnDataType(index);

            DataTypeUtils.callWriteFunction( currAddress,
                    dt,
                    record[index] );

            currAddress += dt.value;

        }

    }

    @Override
    public void insert(IKey key, long srcAddress) {
        long pos = getPosition(key.hashCode());

        if(UNSAFE.getBoolean(null, pos)){
            logger.warning("Overwriting previously written record.");
        }

        UNSAFE.putBoolean(null, pos, true);
        UNSAFE.putInt(null, pos, key.hashCode());
        UNSAFE.copyMemory(null, srcAddress, null, pos + Schema.RECORD_HEADER, schema.getRecordSizeWithoutHeader());
        // this.size++; // this should only be set after commit, so we spread the overhead
    }

    @Override
    public void update(IKey key, Object[] record){
        long pos = getPosition(key.hashCode());

        int maxColumns = this.schema.columnOffset().length;
        long currAddress = pos + Header.SIZE + Integer.BYTES;

        for(int index = 0; index < maxColumns; index++) {

            DataType dt = this.schema.columnDataType(index);

            DataTypeUtils.callWriteFunction( currAddress,
                    dt,
                    record[index] );

            currAddress += dt.value;

        }
    }

    /**
     * The update can be possibly optimized
     * for updating only the fields required
     * instead of the whole record.
     * This method assumes the srcAddress
     * begins with header data
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
    }

    public long address(IKey key) {
        return getPosition(key.hashCode());
    }

    /**
     * Check whether the record is active (if exists)
     */
    @Override
    public boolean exists(IKey key){
        long pos = getPosition(key.hashCode());
        return UNSAFE.getBoolean(null, pos);
    }

    @Override
    public boolean exists(long address){
        return UNSAFE.getBoolean(null, address);
    }

    @Override
    public int size() {
        return this.recordBufferContext.size;
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return new RecordIterator(this.recordBufferContext.address, schema.getRecordSize(),
                this.recordBufferContext.capacity);
    }

    @Override
    public IRecordIterator<IKey> iterator(IKey[] keys) {
        return new KeyRecordIterator(this, keys);
    }

    @Override
    public Object[] record(IKey key) {
        Object[] objectLookup = this.cacheObjectStore.get(key);
        if(objectLookup == null){
            objectLookup = this.readFromIndex(this.address(key));
            this.cacheObjectStore.put( key, objectLookup );
        }
        return objectLookup;
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.UNIQUE;
    }

    public RecordBufferContext buffer(){
        return this.recordBufferContext;
    }

}
