package dk.ku.di.dms.vms.modb.storage.record;

import java.lang.foreign.MemorySegment;

/**
 * This class encapsulates a managed {@link MemorySegment}.
 * It provides the contextual information about the segment of memory being used, like
 * the memory address, how many records can be stored and how many records are stored.
 */
public final class RecordBufferContext {

    // the following metadata should be stored in a metadata buffer for recovery

    // total number of records (of a given schema)
    // the conjunction of all buffers can possibly hold
    public final int capacity;

    // number of records stored so far
    // public int size;

    // just to avoid many method calls, since this value does not change
    public final long address;

    private final MemorySegment memorySegment;

    public RecordBufferContext(MemorySegment memorySegment, int capacity) {
        this.memorySegment = memorySegment;
        this.address = this.memorySegment.address();
        this.capacity = capacity;
    }

    public void force(){
        this.memorySegment.force();
    }

}
