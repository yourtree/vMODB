package dk.ku.di.dms.vms.modb.index;

import dk.ku.di.dms.vms.modb.schema.key.IKey;
import dk.ku.di.dms.vms.modb.schema.key.KeyUtils;
import dk.ku.di.dms.vms.modb.storage.BufferContext;
import dk.ku.di.dms.vms.modb.table.Table;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static dk.ku.di.dms.vms.modb.schema.Header.active;
import static dk.ku.di.dms.vms.modb.schema.Header.inactive;

/**
 * Non-unique hash indexes are used for packing together many related records
 *
 * A single bucket holds all possible hash keys
 * The record bucket is increasingly used as more keys are inserted
 */
public class NonUniqueHashIndex extends AbstractIndex<IKey> {

    private static final Unsafe UNSAFE = Unsafe.getUnsafe();

    private final BufferContext metadataBufferContext;

    private volatile int size;

    // cache of inactive records
    private List<Integer> inactiveLogicalPositions;

    public NonUniqueHashIndex(BufferContext metadataBufferContext,
                              BufferContext recordBufferContext,
                              Table table,
                              int... columnsIndex){
        super(recordBufferContext, table, columnsIndex);
        this.metadataBufferContext = metadataBufferContext;
        this.size = 0;
        this.inactiveLogicalPositions = new ArrayList<>();
    }

    // used by the metadata and record buffers
    private int getLogicalPosition(IKey key, int capacity){
        return (key.hashCode() & 0x7fffffff) % capacity;
    }

    // only used by record buffers... metadata only has one buffer
    private int getBucket(int logicalPosition){
        return (logicalPosition / bufferContext.buffers.length) - 1;
    }

    private int getRecordPhysicalPosition(int logicalPosition){
        // schema size + previous
        return ( (table.getSchema().getRecordSize() + Integer.BYTES)
                    * logicalPosition ) / BUCKET_SIZE;
    }

    private int getMetaPhysicalPosition(int logicalPosition){
        // bit active + number of records in this buffer + latest record logical position
        return ( (Byte.BYTES + Integer.BYTES + Integer.BYTES)
                * logicalPosition );
    }

    private volatile int nextLogicalPosition;

    /**
     * Ordered by latest
     * Easier to manage
     *
     */

    @Override
    public void insert(IKey key, ByteBuffer record) {

        // must get the position of next, if exists
        int logicalPosition = getLogicalPosition(key, metadataBufferContext.capacity);

        int physicalPos = getMetaPhysicalPosition( logicalPosition );

        metadataBufferContext.buffers[0].position(physicalPos);

        // get position of last entry of this hash key
        byte bit = metadataBufferContext.buffers[0].get();

        // then there is already a record
        if(bit == active){

            int numberOfRecords = metadataBufferContext.buffers[0].getInt();
            metadataBufferContext.buffers[0].position(physicalPos + 1);
            metadataBufferContext.buffers[0].putInt( numberOfRecords + 1 );

            // read the latest record logical position
            int logicalRecordPosition = metadataBufferContext.buffers[0].getInt();

            // write the newest position
            metadataBufferContext.buffers[0].position(physicalPos + 1);
            metadataBufferContext.buffers[0].putInt(nextLogicalPosition);

            // optimization: write the bucket together with the record... on next commit!

            // now write the new record
            int newBucket = getBucket(nextLogicalPosition);

            int newPhysicalRecordPos = getRecordPhysicalPosition(nextLogicalPosition);

            bufferContext.buffers[newBucket].position(newPhysicalRecordPos);

            bufferContext.buffers[newBucket].put(record);

            // pointer to previous
            bufferContext.buffers[newBucket].putInt(logicalRecordPosition);

        } else {

            metadataBufferContext.buffers[0].position(physicalPos);
            metadataBufferContext.buffers[0].put(active);
            metadataBufferContext.buffers[0].putInt( 1 ); // first record
            metadataBufferContext.buffers[0].putInt(nextLogicalPosition);

            int newBucket = getBucket(nextLogicalPosition);

            int newPhysicalRecordPos = getRecordPhysicalPosition(nextLogicalPosition);

            bufferContext.buffers[newBucket].position(newPhysicalRecordPos);

            bufferContext.buffers[newBucket].put(record);

            // if first record, previous is -1
            bufferContext.buffers[newBucket].putInt(-1);

        }

        this.nextLogicalPosition++;
        this.size++; // this should only be set after commit, so we spread the overhead
    }

    /**
     * The update can be possibly optimized for updating only the fields required
     * instead of the whole record
     *
     * Optimization: order by key, so binary search could be applied
     * The code now is doing O(M) where M is the average number of records per hash key
     */
    @Override
    public void update(IKey key, ByteBuffer record) {

        // we have to iterate over the records of this hash key
        // we start from the latest
        // it is an implicit linked list

        int logicalPosition = getLogicalPosition(key, metadataBufferContext.capacity);
        int physicalPosition = getMetaPhysicalPosition( logicalPosition );
        metadataBufferContext.buffers[0].position(physicalPosition);
        byte bit = metadataBufferContext.buffers[0].get();

        if(bit == inactive) throw new IllegalStateException("Cannot update inactive hash bucket.");
        // assert (bit == active);

        int numberRecords = metadataBufferContext.buffers[0].getInt();

        // to avoid having to recompute after finding
        int logicalRecordPosition, bucket, physicalRecordPos;
        IKey recordKey;
        ByteBuffer currRecord;

        logicalRecordPosition = metadataBufferContext.buffers[0].getInt();

        boolean found;
        int idx = 0;
        do {

            bucket = getBucket(logicalRecordPosition);

            physicalRecordPos = getRecordPhysicalPosition(logicalRecordPosition);

            // obtaining a view to the specific record
            currRecord = bufferContext.buffers[bucket].slice(physicalRecordPos,
                    (table.getSchema().getRecordSize() + Integer.BYTES) );

            recordKey = KeyUtils.buildRecordKey(table.getSchema(), currRecord, table.getSchema().getPrimaryKeyColumns());

            // build the key from the record read
            found = recordKey.hashCode() == key.hashCode();

            if(!found) {
                // get next in the linked list
                logicalRecordPosition = currRecord.position(table.getSchema().getRecordSize()).getInt();
                idx++;
            }

        } while(!found && idx < numberRecords);// number records to avoid infinite loop

        if(!found) throw new IllegalStateException("Cannot find record in the respective hash bucket.");

        // update
        bufferContext.buffers[bucket].position( physicalPosition );
        bufferContext.buffers[bucket].put(record);

    }



    /**
     * Must also mark the records as inactive
     */
    @Override
    public void delete(IKey key) {
        int logicalPos = getLogicalPosition(key, metadataBufferContext.capacity);
        int bucket = getBucket(logicalPos);
        int physicalPos = getRecordPhysicalPosition(logicalPos);
        bufferContext.buffers[bucket].position(physicalPos);
        bufferContext.buffers[bucket].put(inactive);
        this.size--;
    }

    @Override
    public ByteBuffer retrieve(IKey key) {
        int logicalPos = getLogicalPosition(key, bufferContext.capacity);
        int bucket = getBucket(logicalPos);
        int physicalPos = getRecordPhysicalPosition(logicalPos);
        return bufferContext.buffers[bucket].slice(physicalPos, table.getSchema().getRecordSize() );
    }

    /**
     * Contents are copied into the target buffer
     */
    @Override
    public void retrieve(IKey key, ByteBuffer destBase) {
        int logicalPos = getLogicalPosition(key, metadataBufferContext.capacity);
        int bucket = getBucket(logicalPos);
        int srcOffset = getRecordPhysicalPosition(logicalPos);
        UNSAFE.copyMemory(bufferContext.buffers[bucket], srcOffset, destBase, 0, table.getSchema().getRecordSize());
    }

    /**
     * Must iterate over the
     *
     */
    @Override
    public boolean exists(IKey key){
        int logicalPos = getLogicalPosition(key, metadataBufferContext.capacity);
        int bucket = getBucket(logicalPos);
        int physicalPos = getRecordPhysicalPosition(logicalPos);
        bufferContext.buffers[bucket].position(physicalPos);
        return bufferContext.buffers[bucket].get() != inactive;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.NON_UNIQUE;
    }

}
