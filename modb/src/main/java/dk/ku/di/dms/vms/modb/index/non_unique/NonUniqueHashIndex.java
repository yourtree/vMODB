package dk.ku.di.dms.vms.modb.index.non_unique;

import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.schema.key.IKey;
import dk.ku.di.dms.vms.modb.schema.key.KeyUtils;
import dk.ku.di.dms.vms.modb.storage.OrderedRecordBuffer;
import dk.ku.di.dms.vms.modb.table.Table;

import java.util.Iterator;

/**
 * Space conscious non-unique hash index
 * It manages a sequential buffer for each hash entry
 */
public class NonUniqueHashIndex extends AbstractIndex<IKey> {


    // private volatile int size;

    // better to have a manager. to manage the appendonlybuffer
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
    public long retrieve(IKey key) {
        int bucket = getBucket(key);
        return buffers[bucket].existsAddress(key);
    }

    @Override
    public boolean exists(IKey key){
        int bucket = getBucket(key);
        return buffers[bucket].exists(key);
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.NON_UNIQUE;
    }

    @Override
    public int size() {
        return 0;
    }

}
