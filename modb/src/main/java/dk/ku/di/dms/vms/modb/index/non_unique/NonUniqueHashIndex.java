package dk.ku.di.dms.vms.modb.index.non_unique;

import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;
import dk.ku.di.dms.vms.modb.storage.iterator.BucketIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.definition.Table;

/**
 * Space conscious non-unique hash index
 * It manages a sequential buffer for each hash entry
 */
public class NonUniqueHashIndex extends AbstractIndex<IKey> {


    // private volatile int size;

    // better to have a manager. to manage the append-only buffer
    // correctly (safety)... the manager will expand it if necessary
    // will also deal with deleted records
    // and provide

    private OrderedRecordBuffer[] buffers;

    public NonUniqueHashIndex(OrderedRecordBuffer[] buffers,
                              Table table,
                              int... columnsIndex){
        super(table, columnsIndex);
        this.buffers = buffers;
        // this.size = 0;
    }

    private int getBucket(IKey key){
        return ((key.hashCode() & 0x7fffffff) % buffers.length) - 1;
    }

    private int getBucket(int key){
        return ((key & 0x7fffffff) % buffers.length) - 1;
    }

    /**
     *
     * @param key The key formed by the column values of the record
     * @param srcAddress The source address of the record
     */
    @Override
    public void insert(IKey key, long srcAddress) {
        // must get the position of next, if exists
        int bucket = getBucket(key);
        buffers[bucket].insert( key, srcAddress );
    }

    /**
     * The update can be possibly optimized for updating only the fields required
     * instead of the whole record
     *
     * @param key The key formed by the column values of the record
     * @param srcAddress The new source address of the record
     */
    @Override
    public void update(IKey key, long srcAddress) {

        // get bucket
        int bucket = getBucket(key);

        buffers[bucket].update(key, srcAddress);

    }

    /**
     * Must also mark the records as inactive
     */
    @Override
    public void delete(IKey key) {
        int bucket = getBucket(key);
        buffers[bucket].delete( key );
    }

    @Override
    public boolean exists(IKey key){
        int bucket = getBucket(key);
        return buffers[bucket].exists(key);
    }

    public boolean isBucketEmpty(int key){
        int bucket = getBucket(key);
        return this.buffers[bucket].size() == 0;
    }

    public BucketIterator iterator(){
        return new BucketIterator(this.buffers);
    }

    public RecordBucketIterator iterator(IKey key) {
        int bucket = getBucket(key);
        return new RecordBucketIterator(this.buffers[bucket]);
    }

    public RecordBucketIterator iterator(int key) {
        int bucket = getBucket(key);
        return new RecordBucketIterator(this.buffers[bucket]);
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.NON_UNIQUE;
    }

    @Override
    public NonUniqueHashIndex asNonUniqueHashIndex(){
        return this;
    }

    @Override
    public int size() {
        return 0;
    }

}
