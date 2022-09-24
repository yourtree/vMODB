package dk.ku.di.dms.vms.modb.common.memory;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

/**
 *
 * Goal: Abstract the assigning of memory segments to operators
 *
 * Through this class, operators can obtain memory to
 * store the results of their operation
 *
 * Based on a "claim", this class estimates a possible
 * good memory size
 *
 * Operators can repeatably claim to obtain more memory
 * In this case, the claim must be updated accordingly in order
 * to better benefit from this class
 *
 *
 * Get a portion of memory. Partition in small buckets online (per request basis, from the operators)
 * If runs out, allocates direct byte buffer.
 *
 */
public final class MemoryManager {

    private static final Logger logger = Logger.getLogger("MemoryManager");

    /**
     * Inspiration from JVM class: {@link sun.nio.ch.Util}
     * #getTemporaryDirectBuffer
     * If this class cannot be used, then use it as inspiration?
     */

    /**
     * A cache of direct byte buffers.
     */
    private static class BufferCache {

        // ordered by size
        private SortedSet<ByteBuffer> buffers; // concurrent?

        private BufferCache(){
            this.buffers = new ConcurrentSkipListSet<>( (a,b) -> a.capacity() > b.capacity() ? 1 : 0 );
        }

        public ByteBuffer get(int size) {

            if (buffers.isEmpty()) return null;

            for (ByteBuffer bb : buffers) {
                if (bb.capacity() > size) {
                    boolean res = buffers.remove(bb);
                    if(res) { //  a concurrent call removed it
                        // prepare the buffer and return it
                        bb.rewind();
                        bb.limit(size);
                        return bb;
                    }
                }
            }

            return null;
        }

        public void offer(ByteBuffer buf) {
            this.buffers.add(buf);
        }

    }

    private static final Map<Long, MemoryRefNode> memoryRefCache = new ConcurrentHashMap<>(10);
    private static final Map<Long, MemoryRefNode> assignedMemoryRef = new ConcurrentHashMap<>(10);

    private static final BufferCache bufferCache = new BufferCache();

    private static final Map<Long, ByteBuffer> assignedBuffers = new ConcurrentHashMap<>(10);

    public static MemoryRefNode getTemporaryDirectMemory(long size) {

        MemoryRefNode refNode = memoryRefCache.remove(size);
        if(refNode == null){
            long address = MemoryUtils.getUnsafe().allocateMemory(size);
            MemoryRefNode memRef = new MemoryRefNode(address, size);
            assignedMemoryRef.put( address, memRef );
            return memRef;
        }
        assignedMemoryRef.put( refNode.address(), refNode );
        return refNode;

    }

    public static void releaseTemporaryDirectMemory(long address){
        MemoryRefNode refNode = assignedMemoryRef.remove( address );
        memoryRefCache.put( refNode.bytes(), refNode);
    }

    public static ByteBuffer getTemporaryDirectBuffer(int size) {

        ByteBuffer bb = bufferCache.get(size);
        if(bb == null){
            return ByteBuffer.allocateDirect(size);
        }
        return bb;

    }

    public static ByteBuffer getTemporaryDirectBuffer() {

        ByteBuffer bb = bufferCache.get(1024);
        if(bb == null){
            return ByteBuffer.allocateDirect(1024);
        }
        return bb;

    }

    public static void releaseTemporaryDirectBuffer(ByteBuffer buf) {

        if(buf.position() > 0 || buf.limit() < buf.capacity())
            logger.warning("Buffer returned without being properly cleared!");

        long address = MemoryUtils.getByteBufferAddress(buf);
        assignedBuffers.remove(address);
        bufferCache.offer(buf);
    }

    private static final int DEFAULT_KB = 2048; // 4KB -> 4000 bytes

    public static MemoryRefNode claim(){
        ByteBuffer buf = bufferCache.get(DEFAULT_KB);
        long address = MemoryUtils.getByteBufferAddress(buf);
        assignedBuffers.put(address, buf);
        MemoryRefNode mc = new MemoryRefNode(address, DEFAULT_KB);
        return mc;
    }

    public static MemoryRefNode claim(int bytes){
        ByteBuffer buf = bufferCache.get(bytes);
        long address = MemoryUtils.getByteBufferAddress(buf);
        assignedBuffers.put(address, buf);
        MemoryRefNode mc = new MemoryRefNode(address, bytes);
        return mc;
    }

    /**
     * Must be unclaimed by the {IRepositoryFacade}
     * @param memoryClaimed
     */
    public void unclaim(MemoryRefNode memoryClaimed){
        ByteBuffer buf = assignedBuffers.remove(memoryClaimed.address);
        if(buf != null){
            bufferCache.offer(buf);
        }
    }

}
