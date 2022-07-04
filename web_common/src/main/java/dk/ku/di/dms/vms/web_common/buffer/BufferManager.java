package dk.ku.di.dms.vms.web_common.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Static class encapsulating access to a blocking (unbounded) queue of off-heap buffers
 *
 * Shared safely across threads
 */
public final class BufferManager {

    private static final int DEFAULT_BUFFER_SIZE = 8192;

    private static final BlockingQueue<ByteBuffer> byteBufferQueue = new LinkedBlockingQueue<>();

    /**
     *
     * @return A usable byte buffer
     */
    public static ByteBuffer loanByteBuffer() {

        ByteBuffer buffer = byteBufferQueue.poll();

        // do we have enough bytebuffers?
        if(buffer == null){ // check if a concurrent thread has taken the available buffer
            buffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
        }

        return buffer;
    }

    /**
     *
     * @param buffer returned buffer
     */
    public static void returnByteBuffer(ByteBuffer buffer){
        byteBufferQueue.add(buffer);
    }

    public static ByteBuffer loanByteBuffer(int size){
        return ByteBuffer.allocateDirect(size);
    }

}
