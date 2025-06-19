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
    
    // Add async commit support
    private final boolean enableAsyncCommit = Boolean.parseBoolean(
        System.getProperty("vMODB.iouring.asyncCommit", "true"));
    private final AsyncCommitCoordinator asyncCoordinator;
    
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
        
        // Initialize async coordinator if enabled
        this.asyncCoordinator = enableAsyncCommit ? AsyncCommitCoordinator.getInstance() : null;
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
        
        // Initialize async coordinator if enabled
        this.asyncCoordinator = enableAsyncCommit ? AsyncCommitCoordinator.getInstance() : null;
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
            
            if (enableAsyncCommit && asyncCoordinator != null) {
                // Use synchronous force for critical paths - this maintains semantic contract
                // while still benefiting from async coordinator for batch operations
                        ioUring.queueFsync(ioUringFile, false);
                        ioUring.execute();
            } else {
                // Traditional synchronous fsync
                ioUring.queueFsync(ioUringFile, false);
                ioUring.execute();
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to force data to disk", e);
        }
    }
    
    /**
     * NEW METHOD: Asynchronous force operation that returns immediately
     * This is the key method for enabling non-blocking commit operations
     */
    public java.util.concurrent.CompletableFuture<Void> forceAsync() {
        if (closed) {
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }
        
        if (!enableAsyncCommit || asyncCoordinator == null) {
            // Fallback to synchronous operation
            return java.util.concurrent.CompletableFuture.runAsync(() -> force());
        }
        
        try {
            // First flush any pending writes synchronously
            // (these are typically fast memory operations)
            flushPendingWrites();
            
            // Submit async fsync and return future immediately
            return asyncCoordinator.submitAsyncFsync(ioUringFile, fileName);
            
        } catch (Exception e) {
            java.util.concurrent.CompletableFuture<Void> failedFuture = new java.util.concurrent.CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("Failed to submit async force", e));
            return failedFuture;
        }
    }
    
    /**
     * Batch multiple force operations for maximum efficiency
     */
    public static java.util.concurrent.CompletableFuture<Void> forceBatch(IoUringLoggingHandler... handlers) {
        if (handlers.length == 0) {
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }
        
        // Check if all handlers support async commit
        boolean allAsyncEnabled = true;
        for (IoUringLoggingHandler handler : handlers) {
            if (!handler.enableAsyncCommit || handler.asyncCoordinator == null) {
                allAsyncEnabled = false;
                break;
            }
        }
        
        if (!allAsyncEnabled) {
            // Fallback to individual async operations
            java.util.concurrent.CompletableFuture<Void>[] futures = new java.util.concurrent.CompletableFuture[handlers.length];
            for (int i = 0; i < handlers.length; i++) {
                futures[i] = handlers[i].forceAsync();
            }
            return java.util.concurrent.CompletableFuture.allOf(futures);
        }
        
        try {
            // Flush all pending writes first
            for (IoUringLoggingHandler handler : handlers) {
                handler.flushPendingWrites();
            }
            
            // Prepare batch submission
            IoUringFile[] files = new IoUringFile[handlers.length];
            String[] identifiers = new String[handlers.length];
            
            for (int i = 0; i < handlers.length; i++) {
                files[i] = handlers[i].ioUringFile;
                identifiers[i] = handlers[i].fileName;
            }
            
            // Submit as batch for maximum efficiency
            return handlers[0].asyncCoordinator.submitBatchedFsync(files, identifiers);
            
        } catch (Exception e) {
            java.util.concurrent.CompletableFuture<Void> failedFuture = new java.util.concurrent.CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("Failed to submit batch force", e));
            return failedFuture;
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