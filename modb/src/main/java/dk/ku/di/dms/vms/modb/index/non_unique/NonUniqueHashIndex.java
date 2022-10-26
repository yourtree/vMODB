package dk.ku.di.dms.vms.modb.index.non_unique;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.non_unique.BucketIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.non_unique.NonUniqueRecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Space conscious non-unique hash index
 * It manages a sequential buffer for each hash entry
 */
public final class NonUniqueHashIndex extends AbstractIndex<IKey> {

    // better to have a manager. to manage the append-only buffer
    // correctly (safety)... the manager will expand it if necessary
    // will also deal with deleted records
    // and provide

    private final OrderedRecordBuffer[] buffers;

    private volatile int size;

    private final Map<IKey, List<Object[]>> cacheObjectStore;

    public NonUniqueHashIndex(OrderedRecordBuffer[] buffers,
                              Schema schema,
                              int... columnsIndex){
        super(schema, columnsIndex);
        this.buffers = buffers;
        this.size = 0;
        this.cacheObjectStore = new ConcurrentHashMap<>();
    }

    private int getBucket(IKey key){
        return ((key.hashCode() & 0x7fffffff) % buffers.length) - 1;
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
        int currSize = this.size;
        this.size = currSize + 1;
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

    @Override
    public void insert(IKey key, Object[] record) {
        // TODO finish
    }

    @Override
    public void update(IKey key, Object[] record) {
        // TODO finish
    }

    /**
     * Must also mark the records as inactive
     */
    @Override
    public void delete(IKey key) {
        int bucket = getBucket(key);
        buffers[bucket].delete( key );
        int currSize = this.size;
        this.size = currSize - 1;
    }

    @Override
    public boolean exists(IKey key){
        int bucket = getBucket(key);
        return buffers[bucket].exists(key);
    }

    @Override
    public long address(IKey key) {
        int bucket = getBucket(key);
        return this.buffers[bucket].address();
    }

    public IRecordIterator<IKey> iterator(IKey key) {
        int bucket = getBucket(key);
        return new NonUniqueRecordIterator(new BucketIterator(this.buffers[bucket]));
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return new NonUniqueRecordIterator(new BucketIterator(this.buffers));
    }

    @Override
    public IRecordIterator<IKey> iterator(IKey[] keys) {
        // TODO not sure this makes sense for non unique
        //  but removing this from readonly index would break primary index, etc...
        return null;
    }

//    @Override
//    public List<Object[]> records(IKey key) {
//        List<Object[]> records = this.cacheObjectStore.get(key);
//        if(records == null){
//            IRecordIterator<Long> iterator = this.iterator(key);
//
//            objectLookup = this.readFromIndex(this.address(key));
//            this.cacheObjectStore.put( key, objectLookup );
//        }
//        return objectLookup;
//    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.NON_UNIQUE;
    }

    @Override
    public int size() {
        return this.size;
    }

}
