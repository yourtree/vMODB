package dk.ku.di.dms.vms.modb.storage.iterator;

import dk.ku.di.dms.vms.modb.storage.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;
import sun.misc.Unsafe;

import java.util.Iterator;

public class RecordBucketIterator implements Iterator<long> {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    private final OrderedRecordBuffer buffer;
    private long currPosition;

    public RecordBucketIterator(OrderedRecordBuffer buffer) {
        this.buffer = buffer;
        this.currPosition = buffer.getFirst();
    }

    /**
     * Follows the linked list
     * @return
     */
    @Override
    public boolean hasNext() {
        return UNSAFE.getBoolean(null, currPosition);
    }

    public int key(long address){
        return UNSAFE.getInt(address + OrderedRecordBuffer.deltaKey);
    }

    public int srcAddress(long address){
        return UNSAFE.getInt(address + OrderedRecordBuffer.deltaOffset);
    }

    /**
     * Return the srcAddress of the record
     * @return
     */
    @Override
    public long next() {
        // src address
        long auxCurrPosition = currPosition;
        currPosition = UNSAFE.getLong(currPosition + OrderedRecordBuffer.deltaNext);
        return auxCurrPosition;
    }

}
