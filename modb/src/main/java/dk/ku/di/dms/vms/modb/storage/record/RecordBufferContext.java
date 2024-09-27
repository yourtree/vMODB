package dk.ku.di.dms.vms.modb.storage.record;

import java.lang.foreign.MemorySegment;

/**
 * This class encapsulates a managed {@link MemorySegment}
 */
public final class RecordBufferContext {

    // the following metadata should be stored in a metadata buffer for recovery

    // just to avoid many method calls, since this value does not change
    public final long address;

    private final MemorySegment memorySegment;

    public RecordBufferContext(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
        this.address = this.memorySegment.address();
    }

    public void force(){
        this.memorySegment.force();
    }

}
