package dk.ku.di.dms.vms.modb.storage.iterator;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;
import sun.misc.Unsafe;

public class RecordBucketIterator implements IRecordIterator {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    private long currPosition;

    public RecordBucketIterator(OrderedRecordBuffer buffer) {
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

    public IKey primaryKey(){
        return SimpleKey.of(UNSAFE.getInt(currPosition + OrderedRecordBuffer.deltaKey));
    }

    @Override
    public long current() {
        return currPosition;
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
