package dk.ku.di.dms.vms.modb.storage.iterator;

import dk.ku.di.dms.vms.modb.storage.memory.MemoryUtils;
import sun.misc.Unsafe;

import java.util.Iterator;

/**
 * Iterator tamed for verification of records
 * in sequential positions
 *
 * Cannot be used for iterating over ordered record buffer
 */
public class RecordIterator implements Iterator<long> {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    private long address;
    private final int recordSize;
    private final int capacity;

    private int progress; // how many records have been iterated

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
    public long next() {
        // check for bit active
        while(!UNSAFE.getBoolean(null, address)){
            this.progress++;
            this.address += recordSize;
        }
        long addrToRet = address;
        this.address += recordSize;
        return addrToRet;
    }

    /**
     * Return the key of the current record
     * @return key
     */
    public int key(long address){
        return UNSAFE.getInt(address + 1);
    }

    public int size(){
        return this.capacity;
    }

    public int progress(){
        return this.progress;
    }

}