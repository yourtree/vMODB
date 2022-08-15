package dk.ku.di.dms.vms.modb.storage;

import jdk.incubator.foreign.MemorySegment;

import java.nio.ByteBuffer;

/**
 * Represents the pointers to a buffer holding records
 * The array of buffers is due to the 2 GB limitation
 * of byte buffer addressing.
 *
 * This class is oblivious of how records are stored,
 *  how many records are stored, and the capacity
 *  (maximum number of records permitted).
 *
 * It just carries the pointers to data.
 */
public class RecordBufferContext0 {

    public static final int BUCKET_SIZE = Integer.MAX_VALUE - 8;

    // direct buffers, each max 2 gb (minus 8 bytes)
    public ByteBuffer[] buffers;

    // memory segment of the buffers
    MemorySegment memorySegment;

    public RecordBufferContext0(ByteBuffer[] buffers, MemorySegment memorySegment) {
        this.buffers = buffers;
        this.memorySegment = memorySegment;
    }

}
