package dk.ku.di.dms.vms.modb.storage.record;

import dk.ku.di.dms.vms.modb.storage.memory.MemoryUtils;
import sun.misc.Unsafe;

/**
 * Append-only buffer.
 * A buffer where records are sequentially written.
 * Not hashed as in the hashing technique.
 *
 * An abstraction to avoid the caller to handle the offset
 *
 * It abstracts a pre-assigned portion of the memory
 * for the continuous allocation of records.
 *
 * Limitations:
 * - Non-resizable
 * - Does not support ordering (e.g., by a set of columns)
 *
 * In the future:
 * - Implement bloom filter to support the exists operation (https://notes.volution.ro/v1/2022/07/notes/1290a79c/)
 * - Can also be used for caching writes of transactions
 *
 */
public class AppendOnlyBuffer {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    // what is found in the metadata buffer of this storage structure?
    private final long address;

    // the offset of the bucket
    private long nextOffset;

    // the number of bytes this buffer provides
    private final long capacity;

    public AppendOnlyBuffer(long address, long capacity) {
        this.address = address;
        this.nextOffset = address;
        this.capacity = capacity;
    }

    // move offset and return the previous one
    public void reserve(long bytes){
        this.nextOffset += bytes;
    }

    public long address(){
        return this.address;
    }

    public long capacity(){
        return this.capacity;
    }

    public long nextOffset() {
        return nextOffset;
    }

    /**
     *
     * It is responsibility of the caller to
     * ensure the bounds are respected.
     * This design is to favor performance over safety.
     *
     * The caller must keep track of the remaining space
     * occupied = nextOffset - address - 1
     * remaining = capacity - occupied
     *
     */
    public void append(long srcAddress, long bytes){
        UNSAFE.copyMemory(null, srcAddress, null, nextOffset, bytes);
        this.nextOffset += bytes;
    }

    public void append(long value){
        UNSAFE.putLong(nextOffset, value);
        this.nextOffset += Long.BYTES;
    }

    public void append(int value){
        UNSAFE.putInt(nextOffset, value);
        this.nextOffset += Integer.BYTES;
    }

    public void append(float value){
        UNSAFE.putFloat(nextOffset, value);
        this.nextOffset += Float.BYTES;
    }

    public void copy(long srcAddress, long bytes){
        UNSAFE.copyMemory(null, srcAddress, null,
                nextOffset, bytes);
        this.nextOffset += bytes;
    }

}
