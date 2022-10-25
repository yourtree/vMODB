package dk.ku.di.dms.vms.modb.storage.iterator.unique;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.iterator.CachingKeyIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * Iterator tamed for verification of records in sequential positions
 * Cannot be used for iterating over ordered record buffer
 * Does not consider a record may have multiple items.
 * In other words, it reads from the last consistent snapshot
 */
public final class RecordIterator extends CachingKeyIterator implements IRecordIterator<IKey> {

    private long nextAddress;
    private final int recordSize;
    private final int capacity;
    private int progress; // how many records have been iterated so far

    public RecordIterator(long address, int recordSize, int capacity){
        this.nextAddress = address;
        this.recordSize = recordSize;
        this.capacity = capacity;
        this.progress = 0;
    }

    @Override
    public boolean hasElement() {
        // check for bit active
        while(progress < capacity && !UNSAFE.getBoolean(null, nextAddress)){
            this.progress++;
            this.nextAddress += recordSize;
        }
        return progress < capacity;
    }

    @Override
    public IKey get() {
        return this.keyOf(UNSAFE.getInt(nextAddress + 1));
    }

    @Override
    public void next() {
        this.nextAddress += recordSize;
    }

    @Override
    public long address(){
        return this.nextAddress;
    }

}