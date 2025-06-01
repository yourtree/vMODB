package dk.ku.di.dms.vms.modb.common.config;

/**
 * Bootstrap class for easily enabling IoUring functionality across the system.
 * This provides a simple API to enable IoUring without manually setting system properties.
 */
public final class IoUringBootstrap {
    
    private IoUringBootstrap() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Enable IoUring for all components (storage and logging).
     * This is the simplest way to enable IoUring system-wide.
     */
    public static void enableAll() {
        IoUringConfigurationFactory.enableIoUring();
        printStatus();
    }
    
    /**
     * Enable IoUring with compression for logging and standard IoUring for storage.
     */
    public static void enableAllWithCompression() {
        IoUringConfigurationFactory.enableIoUringWithCompression();
        printStatus();
    }
    
    /**
     * Enable only IoUring storage, keep default logging.
     */
    public static void enableStorageOnly() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "iouring");
        System.setProperty("logging_type", "default");
        printStatus();
    }
    
    /**
     * Enable only IoUring logging, keep default storage.
     */
    public static void enableLoggingOnly() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "default");
        System.setProperty("logging_type", "iouring");
        printStatus();
    }
    
    /**
     * Enable only IoUring compressed logging, keep default storage.
     */
    public static void enableCompressedLoggingOnly() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "default");
        System.setProperty("logging_type", "compressed_iouring");
        printStatus();
    }
    
    /**
     * Disable all IoUring functionality and use default implementations.
     */
    public static void disableAll() {
        IoUringConfigurationFactory.disableIoUring();
        printStatus();
    }
    
    /**
     * Print current IoUring configuration status.
     */
    public static void printStatus() {
        System.out.println("=== IoUring Bootstrap Status ===");
        System.out.println(IoUringConfigurationFactory.getConfigurationSummary());
        System.out.println("==============================");
    }
    
    /**
     * Initialize IoUring based on system properties or default configuration.
     * This method should be called early in application startup.
     */
    public static void initialize() {
        // Check if IoUring should be enabled by default
        String enableByDefault = System.getProperty("iouring.enable_by_default", "false");
        
        if (Boolean.parseBoolean(enableByDefault)) {
            String compressionEnabled = System.getProperty("iouring.compression_enabled", "false");
            if (Boolean.parseBoolean(compressionEnabled)) {
                enableAllWithCompression();
            } else {
                enableAll();
            }
        }
        
        // Always print status for debugging
        printStatus();
    }
} 