package dk.ku.di.dms.vms.modb.storage.iterator.non_unique;

import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.iterator.CachingKeyIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;

/**
 * Encapsulates an iteration over records that belong
 * to a non-unique index hash bucket
 */
public final class RecordBucketIterator extends CachingKeyIterator
        implements IRecordIterator<Long> {

    private long address;

    // how many records have been iterated so far
    // private int progress;

    public RecordBucketIterator(long address) {
        this.address = address;
    }

    /**
     * Follows the linked list
     * @return whether there is a next element
     */
    @Override
    public boolean hasElement() {
        return UNSAFE.getBoolean(null, address);
    }

    public IKey key(){
        return this.keyOf(UNSAFE.getInt(address + OrderedRecordBuffer.deltaKey));
    }

    /**
     *
     * @return the primary key of the record in this bucket
     */
    public IKey primaryKey(){
        long srcAddress = UNSAFE.getLong(address + OrderedRecordBuffer.deltaOffset);
        return this.keyOf(UNSAFE.getInt(srcAddress + Header.SIZE));
    }

    @Override
    public Long get() {
        return this.address;
    }

    public long address() {
        return this.address;
    }

    /**
     * Return the srcAddress of the record
     * @return the next address
     */
    @Override
    public void next() {
        // src address
        this.address = UNSAFE.getLong(this.address + OrderedRecordBuffer.deltaNext);
    }

}
