package dk.ku.di.dms.vms.web_common.buffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Static class encapsulating access to a blocking (unbounded) queue of off-heap buffers
 *
 * Shared safely across threads
 */
public final class BufferManager {

    private static final int DEFAULT_BUFFER_SIZE = 8192;

    private static final BlockingQueue<ByteBuffer> byteBufferQueue = new LinkedBlockingQueue<>();

    private static final AtomicInteger count = new AtomicInteger(1);

    public static RandomAccessFile createMemoryMappedFile(String prefix, long size) throws IOException {

        String fileName = prefix + count.getAndAdd(1);

        File file = new File(fileName);

        if(file.exists()){
            file.delete();
        }

        RandomAccessFile raf =  new RandomAccessFile(fileName, "rw");

        raf.setLength( size );

        // raf.getChannel().transferTo(  )
        return raf;

    }

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
