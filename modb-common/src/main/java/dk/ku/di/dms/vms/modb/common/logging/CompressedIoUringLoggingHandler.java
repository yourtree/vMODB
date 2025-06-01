package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.iouring.IoUring;
import dk.ku.di.dms.vms.modb.iouring.IoUringFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * IoUring-based implementation of ILoggingHandler with LZ4 compression.
 * Combines high-performance asynchronous I/O with data compression.
 * Follows the same compression pattern as CompressedLoggingHandler.
 */
public class CompressedIoUringLoggingHandler implements ILoggingHandler {
    
    private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
    private static final LZ4Compressor LZ4_COMPRESSOR = LZ4_FACTORY.fastCompressor();
    
    // Buffer pool for compression operations - following the same pattern as CompressedLoggingHandler
    private static final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<ByteBuffer>> COMPRESSED_BUFFER_POOL = new ConcurrentHashMap<>();
    
    private final IoUring ioUring;
    private final IoUringFile ioUringFile;
    private final String fileName;
    private final AtomicLong position = new AtomicLong(0);
    private volatile boolean closed = false;
    
    public CompressedIoUringLoggingHandler(String identifier) throws IOException {
        // Initialize io_uring
        this.ioUring = new IoUring();
        
        // Create the file path
        Path logPath = Path.of("logs", identifier + ".compressed.log");
        logPath.getParent().toFile().mkdirs(); // Ensure directory exists
        
        // Store fileName for getFileName() method
        this.fileName = logPath.toString();
        
        // Open file with io_uring
        this.ioUringFile = new IoUringFile(this.fileName);
    }
    
    public CompressedIoUringLoggingHandler(String identifier, IoUring sharedIoUring) throws IOException {
        // Use shared io_uring instance
        this.ioUring = sharedIoUring;
        
        // Create the file path
        Path logPath = Path.of("logs", identifier + ".compressed.log");
        logPath.getParent().toFile().mkdirs(); // Ensure directory exists
        
        // Store fileName for getFileName() method
        this.fileName = logPath.toString();
        
        // Open file with io_uring
        this.ioUringFile = new IoUringFile(this.fileName);
    }
    
    @Override
    public void log(ByteBuffer byteBuffer) throws IOException {
        if (closed) {
            throw new IOException("LoggingHandler is closed");
        }
        
        try {
            // Follow the same compression pattern as CompressedLoggingHandler
            int maxCompressedLength = LZ4_COMPRESSOR.maxCompressedLength(byteBuffer.remaining());
            int key = MemoryUtils.nextPowerOfTwo(maxCompressedLength);
            ConcurrentLinkedQueue<ByteBuffer> targetBufferPool = COMPRESSED_BUFFER_POOL.computeIfAbsent(key, (x) -> new ConcurrentLinkedQueue<>());
            ByteBuffer targetBuffer = targetBufferPool.poll();
            if (targetBuffer == null) {
                targetBuffer = ByteBuffer.allocateDirect(key);
                targetBuffer.position(0);
            }
            
            LZ4_COMPRESSOR.compress(byteBuffer, targetBuffer);
            byteBuffer.rewind();
            targetBuffer.flip();
            
            long offset = position.getAndAdd(targetBuffer.remaining());
            
            // Queue asynchronous write - write until all data is written, like DefaultLoggingHandler
            do {
                ioUring.queueWrite(ioUringFile, targetBuffer, offset + (targetBuffer.limit() - targetBuffer.remaining()));
                ioUring.execute();
            } while (targetBuffer.hasRemaining());
            
            // Return buffer to pool
            targetBuffer.clear();
            COMPRESSED_BUFFER_POOL.get(key).add(targetBuffer);
            
        } catch (Exception e) {
            throw new IOException("Failed to log compressed data", e);
        }
    }
    
    @Override
    public void force() {
        if (closed) {
            return;
        }
        
        try {
            // Queue fsync operation for data integrity
            ioUring.queueFsync(ioUringFile, false);
            
            // Execute the fsync
            ioUring.execute();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to force data to disk", e);
        }
    }
    
    @Override
    public void close() {
        if (closed) {
            return;
        }
        
        try {
            // Force any pending data to disk
            force();
            
            // Close the file
            if (ioUringFile != null) {
                ioUringFile.close();
            }
            
            closed = true;
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to close compressed logging handler", e);
        }
    }
    
    @Override
    public String getFileName() {
        return this.fileName;
    }
} 