package dk.ku.di.dms.vms.modb.storage.iterator;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import sun.misc.Unsafe;

/**
 * Iterator tamed for verification of records
 * in sequential positions
 *
 * Cannot be used for iterating over ordered record buffer
 */
public class RecordIterator extends CachingKeyIterator implements IRecordIterator {

    protected static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    protected long address;
    protected final int recordSize;
    private final int capacity;

    protected int progress; // how many records have been iterated

    public RecordIterator(long address, int recordSize, int capacity){
        this.address = address;
        this.recordSize = recordSize;
        this.capacity = capacity;
        this.progress = 0;
    }

    @Override
    public boolean hasNext() {
        return progress < capacity;
    }

    /**
     * This method should always comes after a hasNext call
     * @return the record address
     */
    @Override
    public Long next() {
        // check for bit active
        while(!UNSAFE.getBoolean(null, address)){
            this.progress++;
            this.address += recordSize;
        }
        long addressToRet = address;
        this.address += recordSize;
        return addressToRet;
    }

    /**
     * Return the key of the current record
     * @return key
     */
    @Override
    public IKey primaryKey(){
        return this.keyOf(UNSAFE.getInt(address + 1));
    }

    @Override
    public long current(){
        return this.address;
    }

    public int size(){
        return this.capacity;
    }

}