package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.iouring.IoUring;
import dk.ku.di.dms.vms.modb.iouring.IoUringFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * IoUring-based implementation of ILoggingHandler.
 * Uses asynchronous I/O with io_uring for high-performance logging.
 */
public class IoUringLoggingHandler implements ILoggingHandler {
    
    private final IoUring ioUring;
    private final IoUringFile ioUringFile;
    private final String fileName;
    private final AtomicLong position = new AtomicLong(0);
    private volatile boolean closed = false;
    
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
            
            // Queue asynchronous write - write until all data is written, like DefaultLoggingHandler
            do {
                ioUring.queueWrite(ioUringFile, byteBuffer, offset + (dataSize - byteBuffer.remaining()));
                ioUring.execute();
            } while (byteBuffer.hasRemaining());
            
        } catch (Exception e) {
            throw new IOException("Failed to log data", e);
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
} 