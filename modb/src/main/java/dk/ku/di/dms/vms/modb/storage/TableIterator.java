package dk.ku.di.dms.vms.modb.storage;

import dk.ku.di.dms.vms.modb.table.Table;
import sun.misc.Unsafe;

import java.util.Iterator;

public class TableIterator implements Iterator<long> {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    private final Table table;

    private long address;
    private final int recordSize;
    private final int capacity;

    private int progress; // how many records have been iterated

    public TableIterator(Table table, long address, int recordSize, int capacity){
        this.table = table;
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

    public int size(){
        return this.capacity;
    }

    public int progress(){
        return this.progress;
    }

    public Table table(){
        return this.table;
    }

}
