package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;

/**
 * Only allowed values are exposed as part of the iteration
 */
public class ConsistentRecordIterator extends RecordIterator {

    public ConsistentRecordIterator(long address, int recordSize, int capacity) {
        super(address, recordSize, capacity);
    }

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

}
