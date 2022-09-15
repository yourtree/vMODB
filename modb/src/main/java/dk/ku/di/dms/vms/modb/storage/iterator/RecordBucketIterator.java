package dk.ku.di.dms.vms.modb.storage.iterator;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;
import sun.misc.Unsafe;

public class RecordBucketIterator extends CachingKeyIterator implements IRecordIterator {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    private long currPosition;

    public RecordBucketIterator(OrderedRecordBuffer buffer) {
        this.currPosition = buffer.getFirst();
    }

    /**
     * Follows the linked list
     * @return whether there is a next element
     */
    @Override
    public boolean hasNext() {
        return UNSAFE.getBoolean(null, currPosition);
    }

    public IKey primaryKey(){
        return this.keyOf(UNSAFE.getInt(currPosition + OrderedRecordBuffer.deltaKey));
    }

    @Override
    public long current() {
        return currPosition;
    }

    /**
     * Return the srcAddress of the record
     * @return the next address
     */
    @Override
    public Long next() {
        // src address
        long auxCurrPosition = currPosition;
        currPosition = UNSAFE.getLong(currPosition + OrderedRecordBuffer.deltaNext);
        return auxCurrPosition;
    }

}
