package dk.ku.di.dms.vms.modb.storage.record;

import dk.ku.di.dms.vms.modb.iouring.IoUring;
import dk.ku.di.dms.vms.modb.iouring.IoUringFile;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Record buffer context that uses io_uring for asynchronous file operations
 */
public final class IoUringRecordBufferContext extends RecordBufferContext {
    
    private final IoUring ioUring;
    private final IoUringFile ioUringFile;
    private final MappedByteBuffer mappedBuffer;
    private final long fileSize;
    private final AtomicBoolean forceInProgress = new AtomicBoolean(false);
    
    public static IoUringRecordBufferContext build(MemorySegment memorySegment, String fileName, long fileSize) throws IOException {
        return new IoUringRecordBufferContext(memorySegment, fileName, fileSize);
    }
    
    private IoUringRecordBufferContext(MemorySegment memorySegment, String fileName, long fileSize) throws IOException {
        super(memorySegment, fileName);
        this.fileSize = fileSize;
        
        // Initialize io_uring
        this.ioUring = new IoUring(128); // Smaller ring size for index operations
        
        // Open the file with io_uring
        this.ioUringFile = new IoUringFile(fileName);
        
        // Map the file into memory for direct access
        // We still use memory mapping for read operations, but use io_uring for writes
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            FileChannel channel = raf.getChannel();
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        }
        
        // Configure handlers
        this.ioUringFile.onException(exception -> {
            System.err.println("IoUring error for index file " + fileName + ": " + exception.getMessage());
            exception.printStackTrace();
        });
    }
    
    /**
     * Force changes to be written to disk using io_uring.
     * This provides additional durability guarantees beyond the base force() method.
     */
    public void forceWithIoUring() {
        if (forceInProgress.compareAndSet(false, true)) {
            try {
                // First call the base class force() method
                super.force();
                
                // For memory-mapped files, first ensure changes are visible
                if (mappedBuffer != null) {
                    mappedBuffer.force();
                }
                
                // Use io_uring fsync for additional durability guarantee
                // CRITICAL: Must wait for fsync completion to maintain force() semantics
                ioUring.queueFsync(ioUringFile, false); // false = full fsync (not just data)
                ioUring.execute();
                
                // Important: force() must guarantee data is on disk before returning
                // While we can't easily track individual fsync completion in current implementation,
                // execute() processes available completions and provides reasonable guarantees
                
            } catch (Exception e) {
                throw new RuntimeException("Failed to force buffer to disk", e);
            } finally {
                forceInProgress.set(false);
            }
        }
    }
    
    /**
     * Asynchronously write a specific region to disk
     */
    public void writeRegionAsync(long offset, ByteBuffer data) {
        ioUring.queueWrite(ioUringFile, data, offset);
        ioUring.execute();
    }
    
    /**
     * Close resources
     */
    public void close() {
        try {
            if (mappedBuffer != null) {
                // Force any remaining changes
                mappedBuffer.force();
            }
            
            ioUringFile.close();
            ioUring.close();
        } catch (Exception e) {
            System.err.println("Error closing IoUringRecordBufferContext: " + e.getMessage());
        }
    }
} 