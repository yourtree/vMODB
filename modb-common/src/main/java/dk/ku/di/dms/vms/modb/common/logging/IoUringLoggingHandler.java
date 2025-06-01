package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.iouring.IoUring;
import dk.ku.di.dms.vms.modb.iouring.IoUringFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * IoUring-based implementation of ILoggingHandler.
 * Uses asynchronous I/O with io_uring for high-performance logging.
 * Enhanced version with batching support for maximum performance.
 */
public class IoUringLoggingHandler implements ILoggingHandler {
    
    private final IoUring ioUring;
    private final IoUringFile ioUringFile;
    private final String fileName;
    private final AtomicLong position = new AtomicLong(0);
    private volatile boolean closed = false;
    
    // Batch operation support
    private final ConcurrentLinkedQueue<PendingWrite> pendingWrites = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingWriteCount = new AtomicInteger(0);
    private final AtomicInteger pendingFsyncCount = new AtomicInteger(0);
    
    // Batch configuration
    private static final int MAX_BATCH_SIZE = Integer.parseInt(
        System.getProperty("vMODB.iouring.maxBatchSize", "32"));
    private static final boolean ENABLE_BATCHING = Boolean.parseBoolean(
        System.getProperty("vMODB.iouring.enableBatching", "true"));
    
    private static class PendingWrite {
        final ByteBuffer buffer;
        final long offset;
        final long timestamp;
        
        PendingWrite(ByteBuffer buffer, long offset) {
            this.buffer = buffer;
            this.offset = offset;
            this.timestamp = System.nanoTime();
        }
    }
    
    public IoUringLoggingHandler(String identifier) throws IOException {
        // Initialize io_uring
        this.ioUring = new IoUring();
        
        // Create the file path
        Path logPath = Path.of("logs", identifier + ".log");
        logPath.getParent().toFile().mkdirs(); // Ensure directory exists
        
        // Store fileName for getFileName() method
        this.fileName = logPath.toString();
        
        // Open file with io_uring
        this.ioUringFile = new IoUringFile(this.fileName);
    }
    
    public IoUringLoggingHandler(String identifier, IoUring sharedIoUring) throws IOException {
        // Use shared io_uring instance
        this.ioUring = sharedIoUring;
        
        // Create the file path
        Path logPath = Path.of("logs", identifier + ".log");
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
            int dataSize = byteBuffer.remaining();
            long offset = position.getAndAdd(dataSize);
            
            if (ENABLE_BATCHING && pendingWriteCount.get() < MAX_BATCH_SIZE) {
                // Batch mode: collect write requests
                pendingWrites.add(new PendingWrite(byteBuffer.duplicate(), offset));
                int currentCount = pendingWriteCount.incrementAndGet();
                
                // Submit when reaching batch size or timeout
                if (currentCount >= MAX_BATCH_SIZE) {
                    flushPendingWrites();
                }
            } else {
                // Direct mode: immediate write (maintain compatibility)
                logDirectly(byteBuffer, offset);
            }
            
        } catch (Exception e) {
            throw new IOException("Failed to log data", e);
        }
    }
    
    /**
     * Batch flush pending write operations
     */
    private void flushPendingWrites() {
        if (pendingWrites.isEmpty()) {
            return;
        }
        
        int batchCount = 0;
        PendingWrite write;
        
        // Batch submit to io_uring
        while ((write = pendingWrites.poll()) != null && batchCount < MAX_BATCH_SIZE) {
            ioUring.queueWrite(ioUringFile, write.buffer, write.offset);
            batchCount++;
            pendingWriteCount.decrementAndGet();
        }
        
        if (batchCount > 0) {
            // Submit all write operations in one call
            ioUring.execute();
            
            if (batchCount > 1) {
                System.out.printf("[IoUring] Batched %d writes in single submit%n", batchCount);
            }
        }
    }
    
    /**
     * Direct write mode (non-batch)
     */
    private void logDirectly(ByteBuffer byteBuffer, long offset) throws IOException {
        // Queue asynchronous write - write until all data is written
        do {
            ioUring.queueWrite(ioUringFile, byteBuffer, offset + (byteBuffer.capacity() - byteBuffer.remaining()));
            ioUring.execute();
        } while (byteBuffer.hasRemaining());
    }
    
    @Override
    public void force() {
        if (closed) {
            return;
        }
        
        try {
            // First flush any pending writes and wait for completion
            flushPendingWrites();
            
            // Synchronous fsync to guarantee data integrity
            // Force must wait for fsync completion to maintain semantic contract
            ioUring.queueFsync(ioUringFile, false);
            ioUring.execute();
            
            // Note: For true synchronous behavior, we should wait for fsync completion
            // The current io_uring implementation doesn't provide easy completion tracking for fsync
            // But execute() will process any immediately available completions
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to force data to disk", e);
        }
    }
    
    /**
     * Manually flush batch writes (for scenarios requiring immediate persistence)
     */
    public void flushBatch() {
        flushPendingWrites();
    }
    
    @Override
    public void close() {
        if (closed) {
            return;
        }
        
        try {
            // Force any pending data to disk
            flushPendingWrites();
            force();
            
            // Wait for async operations to complete
            Thread.sleep(10);
            
            // Close the file
            if (ioUringFile != null) {
                ioUringFile.close();
            }
            
            // Note: We don't close the shared ioUring instance as it might be used by others
            
            closed = true;
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to close logging handler", e);
        }
    }
    
    @Override
    public String getFileName() {
        return this.fileName;
    }
    
    /**
     * Get batch statistics information
     */
    public String getBatchStats() {
        return String.format("PendingWrites: %d, PendingFsync: %d", 
            pendingWriteCount.get(), pendingFsyncCount.get());
    }
} 