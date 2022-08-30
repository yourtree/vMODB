package dk.ku.di.dms.vms.modb.storage.record;

import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.common.meta.MemoryUtils;
import sun.misc.Unsafe;

import java.util.List;
import java.util.Map;

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

    /**
     * "Append" operations to support scan
     * @param srcAddress the source address of the record
     */
    public void append(long srcAddress){
        UNSAFE.putLong(nextOffset, srcAddress);
        this.nextOffset += Long.BYTES;
    }

    /**
     * Data type can repeat, but address cannot
     * Address points to the record column offset
     *
     * A possible optimization is verifying whether
     * there are subsequent columns
     * so they can be copied together
     *
     * @param values the map
     */
    public void append(List<Map.Entry<long, DataType>> values){
        DataType dt;
        for(Map.Entry<long, DataType> entry : values) {
            dt = entry.getValue();
            UNSAFE.copyMemory( entry.getKey(), this.nextOffset, dt.value );
            this.nextOffset += dt.value;
        }
    }

}
