package dk.ku.di.dms.vms.modb.storage;

import dk.ku.di.dms.vms.modb.storage.record.IoUringRecordBufferContext;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import java.lang.foreign.MemorySegment;

/**
 * Factory class for creating IoUring-based storage components.
 * This provides a centralized way to enable/disable IoUring storage functionality
 * without modifying existing code.
 */
public final class IoUringStorageFactory {
    
    private static final String IOURING_ENABLED_PROPERTY = "iouring.enabled";
    private static final String STORAGE_BACKEND_PROPERTY = "storage_backend";
    
    private IoUringStorageFactory() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Check if IoUring is globally enabled.
     */
    public static boolean isIoUringEnabled() {
        return Boolean.parseBoolean(System.getProperty(IOURING_ENABLED_PROPERTY, "false"));
    }
    
    /**
     * Check if IoUring storage backend is enabled.
     */
    public static boolean isIoUringStorageEnabled() {
        if (!isIoUringEnabled()) {
            return false;
        }
        String storageBackend = System.getProperty(STORAGE_BACKEND_PROPERTY, "default");
        return "iouring".equals(storageBackend);
    }
    
    /**
     * Create a RecordBufferContext with IoUring support if enabled.
     * Falls back to standard implementation if IoUring is disabled or fails.
     */
    public static RecordBufferContext createRecordBufferContext(
            MemorySegment memorySegment, String fileName, long fileSize) {
        
        if (isIoUringStorageEnabled()) {
            try {
                return IoUringRecordBufferContext.build(memorySegment, fileName, fileSize);
            } catch (Exception e) {
                System.err.println("Failed to create IoUring RecordBufferContext, falling back to default: " + e.getMessage());
            }
        }
        
        // Fallback to standard implementation
        return RecordBufferContext.build(memorySegment, fileName);
    }
    
    /**
     * Create a RecordBufferContext without IoUring (for cases where file size is unknown).
     */
    public static RecordBufferContext createRecordBufferContext(
            MemorySegment memorySegment, String fileName) {
        return RecordBufferContext.build(memorySegment, fileName);
    }
    
    /**
     * Enable IoUring storage.
     */
    public static void enableIoUringStorage() {
        System.setProperty(IOURING_ENABLED_PROPERTY, "true");
        System.setProperty(STORAGE_BACKEND_PROPERTY, "iouring");
    }
    
    /**
     * Disable IoUring storage and use default implementation.
     */
    public static void disableIoUringStorage() {
        System.setProperty(STORAGE_BACKEND_PROPERTY, "default");
    }
} 