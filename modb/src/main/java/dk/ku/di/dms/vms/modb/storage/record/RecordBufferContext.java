package dk.ku.di.dms.vms.modb.storage.record;

import java.lang.foreign.MemorySegment;

/**
 * This class encapsulates a managed {@link MemorySegment}
 */
public sealed class RecordBufferContext permits AppendOnlyBuffer {

    // just to avoid many method calls, since this value does not change
    public final long address;

    protected final MemorySegment memorySegment;

    public RecordBufferContext(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
        this.address = this.memorySegment.address();
    }

    public final void force(){
        this.memorySegment.force();
    }

}
