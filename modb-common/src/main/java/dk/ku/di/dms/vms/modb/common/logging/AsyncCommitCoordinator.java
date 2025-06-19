package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.logging.AsyncCommitCoordinator.CommitRequest;
import dk.ku.di.dms.vms.modb.iouring.IoUring;
import dk.ku.di.dms.vms.modb.iouring.IoUringFile;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous commit coordinator that leverages io_uring for non-blocking commit operations.
 * This allows transaction threads to continue processing while log persistence happens asynchronously.
 */
public class AsyncCommitCoordinator {
    
    private static final System.Logger LOGGER = System.getLogger(AsyncCommitCoordinator.class.getName());
    
    // Singleton pattern for minimal integration impact
    private static volatile AsyncCommitCoordinator INSTANCE;
    private static final Object INSTANCE_LOCK = new Object();
    
    private final IoUring ioUring;
    private final Thread processingThread;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong completedCommits = new AtomicLong(0);
    
    // Queue for pending requests - single threaded processing
    private final ConcurrentLinkedQueue<CommitRequest> pendingRequests = new ConcurrentLinkedQueue<>();
    
    public static AsyncCommitCoordinator getInstance() {
        if (INSTANCE == null) {
            synchronized (INSTANCE_LOCK) {
                if (INSTANCE == null) {
                    try {
                        INSTANCE = new AsyncCommitCoordinator();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to initialize AsyncCommitCoordinator", e);
                    }
                }
            }
        }
        return INSTANCE;
    }
    
    /**
     * Represents a commit request with completion tracking
     */
    public static class CommitRequest {
        final IoUringFile file;
        final CompletableFuture<Void> completion;
        final long requestId;
        final String identifier;
        final boolean isBatch;
        final IoUringFile[] batchFiles;
        final String[] batchIdentifiers;
        
        // Single file request
        CommitRequest(IoUringFile file, String identifier) {
            this.file = file;
            this.completion = new CompletableFuture<>();
            this.requestId = System.nanoTime();
            this.identifier = identifier;
            this.isBatch = false;
            this.batchFiles = null;
            this.batchIdentifiers = null;
        }
        
        // Batch request
        CommitRequest(IoUringFile[] files, String[] identifiers) {
            this.file = null;
            this.completion = new CompletableFuture<>();
            this.requestId = System.nanoTime();
            this.identifier = "batch-" + files.length;
            this.isBatch = true;
            this.batchFiles = files;
            this.batchIdentifiers = identifiers;
        }
    }
    
    private AsyncCommitCoordinator() {
        // Create dedicated io_uring instance for async commits
        this.ioUring = new IoUring(256);
        
        // Use single thread to process IoUring operations - no concurrency issues
        this.processingThread = Thread.ofPlatform()
            .name("iouring-async-processor")
            .start(this::processRequests);
            
        LOGGER.log(System.Logger.Level.INFO, "AsyncCommitCoordinator initialized with single-threaded processing");
    }
    
    /**
     * Submit an asynchronous fsync request.
     * Returns immediately with a CompletableFuture for completion tracking.
     */
    public CompletableFuture<Void> submitAsyncFsync(IoUringFile file, String identifier) {
        CommitRequest request = new CommitRequest(file, identifier);
        pendingRequests.offer(request);
        
        LOGGER.log(System.Logger.Level.DEBUG, 
            "Queued async fsync for {}, requestId: {}", 
            identifier, request.requestId);
        
        return request.completion;
    }
    
    /**
     * Submit multiple fsync operations in a batch for maximum efficiency
     */
    public CompletableFuture<Void> submitBatchedFsync(IoUringFile[] files, String[] identifiers) {
        if (files.length != identifiers.length) {
            throw new IllegalArgumentException("Files and identifiers arrays must have same length");
        }
        
        CommitRequest request = new CommitRequest(files, identifiers);
        pendingRequests.offer(request);
        
        LOGGER.log(System.Logger.Level.DEBUG, "Queued batched fsync for {} files", files.length);
        
        return request.completion;
    }
    
    /**
     * Single threaded processing loop - no concurrency issues with IoUring
     */
    private void processRequests() {
        LOGGER.log(System.Logger.Level.INFO, "AsyncCommitCoordinator processing thread started");
        
        while (running.get() || !pendingRequests.isEmpty()) {
            try {
                CommitRequest request = pendingRequests.poll();
                
                if (request != null) {
                    processRequest(request);
                } else {
                    // No requests, sleep briefly
                    Thread.sleep(1);
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.ERROR, "Error in processing thread", e);
            }
        }
        
        LOGGER.log(System.Logger.Level.INFO, "AsyncCommitCoordinator processing thread ended");
    }
    
    private void processRequest(CommitRequest request) {
        try {
            if (request.isBatch) {
                // Process batch request
                LOGGER.log(System.Logger.Level.DEBUG, 
                    "Processing batch fsync for {} files, requestId: {}", 
                    request.batchFiles.length, request.requestId);
                
                // Queue all fsync operations
                for (IoUringFile file : request.batchFiles) {
                    ioUring.queueFsync(file, false);
                }
                
                // Execute and wait for completion
                int completions = ioUring.execute();
                
                LOGGER.log(System.Logger.Level.DEBUG, 
                    "Batch fsync completed, requestId: {}, completions: {}", 
                    request.requestId, completions);
                
                completedCommits.addAndGet(request.batchFiles.length);
                request.completion.complete(null);
                
            } else {
                // Process single request
                LOGGER.log(System.Logger.Level.DEBUG, 
                    "Processing async fsync for {}, requestId: {}", 
                    request.identifier, request.requestId);
                
                // Queue and execute the fsync operation
                ioUring.queueFsync(request.file, false);
                int completions = ioUring.execute();
                
                LOGGER.log(System.Logger.Level.DEBUG, 
                    "Fsync completed for {}, requestId: {}, completions: {}", 
                    request.identifier, request.requestId, completions);
                
                completedCommits.incrementAndGet();
                request.completion.complete(null);
            }
            
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, 
                "Failed to process request {}", request.requestId, e);
            request.completion.completeExceptionally(e);
        }
    }
    
    /**
     * Get statistics about commit operations
     */
    public String getStats() {
        return String.format("Pending: %d, Completed: %d", 
            pendingRequests.size(), completedCommits.get());
    }
    
    /**
     * Graceful shutdown
     */
    public void shutdown() {
        running.set(false);
        
        try {
            processingThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        try {
            ioUring.close();
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.WARNING, "Error closing io_uring", e);
        }
    }
} 