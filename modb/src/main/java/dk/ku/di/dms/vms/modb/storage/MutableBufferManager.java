package dk.ku.di.dms.vms.modb.storage;

import sun.misc.Unsafe;

import java.util.*;

import static dk.ku.di.dms.vms.modb.schema.Header.inactive;

/**
 * Provides safety guarantees on managing main-memory buffers
 *
 * For instance, the non-unique index does not need to worry about
 * the safety on writing to the buffer
 *
 * It tracks the deleted records so the space can be reused efficiently
 *
 */
public class MutableBufferManager {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    // metadata of this buffer manager
    // the set of deleted records.. last in first out for decreasing the chance of paging
    //
//     * - Keep track of deleted records, in this case,
//     deleted records are overwritten by new
    private final LinkedList<Long> deletedOffsets;

    private final AppendOnlyBuffer buffer;

    // how much is occupied
    // it also counts the deleted records
    // to make it agnostic
    private byte bytesWritten = 0;

    // the number of active writes. i.e., number of active records
    private int usefulWrites = 0;

    public MutableBufferManager(AppendOnlyBuffer buffer){
        this.buffer = buffer;
        this.deletedOffsets = new LinkedList<>();
    }

    public boolean isEmpty(){

        // is there any way to assert whether all records are deleted?
        // this manager is Header aware, that is, different records could have different sizes
        // so the number of usefulWrites

        if(usefulWrites > 0){
            return false;
        }
        return true;
    }

    public void delete(long destOffset){
        deletedOffsets.addLast(destOffset);
        usefulWrites--;
        UNSAFE.putByte(destOffset, inactive);
    }

    public void insert(long srcOffset, long bytes){

        // overwriting
        if(!deletedOffsets.isEmpty()){
            long destOffset = deletedOffsets.removeLast();
            UNSAFE.copyMemory(null, srcOffset, null, destOffset, bytes);
            usefulWrites++;
            return;
        }

        if(buffer.capacity() - (bytesWritten + bytes) >= 0){
            buffer.append(srcOffset, bytes);
            this.bytesWritten += bytes;
            usefulWrites++;
        }
        // else log
    }

    public long capacity(){
        return this.buffer.capacity();
    }

    public long address(){
        return this.buffer.address();
    }

    public int usefulWrites() {
        return usefulWrites;
    }
}
