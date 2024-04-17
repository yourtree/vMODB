package dk.ku.di.dms.vms.modb.storage.record;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * This class maintains important information
 * regarding the managed {@link MemorySegment}.
 * -
 * It provides the contextual information about
 * the segment of memory being used, like
 * how many records can be stored,
 * how many records are stored,
 * and ...
 *
 */
public class RecordBufferContext {

    // the following metadata should be stored in the metadata buffer for recovery

    // total number of records (of a given schema)
    // the conjunction of all buffers can possibly hold
    public final int capacity;

    // number of records stored so far
    // public int size;

    // just to avoid many method calls, since this value does not change
    public final long address;

    /**
     * Contain entries related to the metadata of the records stored in the record buffer.
     * Each type of index may use this buffer in a particular way
     */
    // public ByteBuffer metadataBuffer;

    private MemorySegment memorySegment;

    private ByteBuffer byteBuffer;

    public RecordBufferContext(MemorySegment memorySegment, int capacity) {
        this.memorySegment = memorySegment;
        this.address = this.memorySegment.address();
        this.capacity = capacity;
    }

    public RecordBufferContext(ByteBuffer byteBuffer, long address, int capacity) {
        this.byteBuffer = byteBuffer;
        this.address = address;
        this.capacity = capacity;
    }

    public void log(){
        this.memorySegment.force();
    }

}
