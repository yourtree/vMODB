package dk.ku.di.dms.vms.modb.storage.iterator;

import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;

import java.util.Iterator;

/**
 * Encapsulates the iteration over buckets of a non unique hash index
 */
public class BucketIterator implements Iterator<RecordBucketIterator> {

    private final int size;

    private final OrderedRecordBuffer[] buffers;

    private int progress; // how many records have been iterated

    public BucketIterator(OrderedRecordBuffer[] buffers){
        this.buffers = buffers;
        this.size = buffers.length;
        this.progress = 0;
    }

    @Override
    public boolean hasNext() {
        return progress < size;
    }

    /**
     * This method should always comes after a hasNext call
     * @return the record address
     */
    @Override
    public RecordBucketIterator next() {

        while(progress < size && buffers[progress].size() < 0){
            this.progress++;
        }
        return new RecordBucketIterator(this.buffers[progress]);

    }

    public int progress(){
        return this.progress;
    }

}
