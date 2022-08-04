package dk.ku.di.dms.vms.modb.index;

import jdk.incubator.foreign.MemorySegment;

import java.nio.ByteBuffer;

public class BufferContext {

    // total number of records the conjunction of all buffers can possibly hold
    public int capacity;

    // direct buffers, each max 2 gb (minus 8 bytes)
    public ByteBuffer[] buffers;

    // memory segment of the buffers
    MemorySegment memorySegment;

    public BufferContext(int capacity, ByteBuffer[] buffers, MemorySegment memorySegment) {
        this.capacity = capacity;
        this.buffers = buffers;
        this.memorySegment = memorySegment;
    }
}
