package dk.ku.di.dms.vms.modb.common.config;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;

/**
 * Factory class for creating IoUring-based components.
 * This provides a centralized way to enable/disable IoUring functionality
 * without modifying existing code.
 */
public final class IoUringConfigurationFactory {
    
    private static final String IOURING_ENABLED_PROPERTY = "iouring.enabled";
    private static final String STORAGE_BACKEND_PROPERTY = "storage_backend";
    private static final String LOGGING_TYPE_PROPERTY = "logging_type";
    
    // Cached configuration values
    private static final boolean IOURING_ENABLED = isIoUringEnabled();
    private static final boolean IOURING_STORAGE_ENABLED = isIoUringStorageEnabled();
    private static final boolean IOURING_LOGGING_ENABLED = isIoUringLoggingEnabled();
    
    private IoUringConfigurationFactory() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Check if IoUring is globally enabled.
     * This is controlled by the system property 'iouring.enabled'.
     */
    public static boolean isIoUringEnabled() {
        return Boolean.parseBoolean(System.getProperty(IOURING_ENABLED_PROPERTY, "false"));
    }
    
    /**
     * Check if IoUring storage backend is enabled.
     * This checks both global IoUring setting and specific storage backend setting.
     */
    public static boolean isIoUringStorageEnabled() {
        if (!IOURING_ENABLED) {
            return false;
        }
        String storageBackend = System.getProperty(STORAGE_BACKEND_PROPERTY, "default");
        return "iouring".equals(storageBackend);
    }
    
    /**
     * Check if IoUring logging is enabled.
     * This checks both global IoUring setting and specific logging type setting.
     */
    public static boolean isIoUringLoggingEnabled() {
        if (!IOURING_ENABLED) {
            return false;
        }
        String loggingType = System.getProperty(LOGGING_TYPE_PROPERTY, "default");
        return "iouring".equals(loggingType) || "compressed_iouring".equals(loggingType);
    }
    
    /**
     * Create a logging handler with IoUring support if enabled.
     * Falls back to standard implementation if IoUring is disabled or fails.
     * Note: This method delegates to LoggingHandlerBuilder for actual creation.
     */
    public static ILoggingHandler createLoggingHandler(String identifier) {
        return LoggingHandlerBuilder.build(identifier);
    }
    
    /**
     * Enable IoUring globally.
     * This method can be called programmatically to enable IoUring without setting system properties.
     */
    public static void enableIoUring() {
        System.setProperty(IOURING_ENABLED_PROPERTY, "true");
        System.setProperty(STORAGE_BACKEND_PROPERTY, "iouring");
        System.setProperty(LOGGING_TYPE_PROPERTY, "iouring");
    }
    
    /**
     * Enable IoUring with compression for logging.
     */
    public static void enableIoUringWithCompression() {
        System.setProperty(IOURING_ENABLED_PROPERTY, "true");
        System.setProperty(STORAGE_BACKEND_PROPERTY, "iouring");
        System.setProperty(LOGGING_TYPE_PROPERTY, "compressed_iouring");
    }
    
    /**
     * Disable IoUring and use default implementations.
     */
    public static void disableIoUring() {
        System.setProperty(IOURING_ENABLED_PROPERTY, "false");
        System.setProperty(STORAGE_BACKEND_PROPERTY, "default");
        System.setProperty(LOGGING_TYPE_PROPERTY, "default");
    }
    
    /**
     * Get configuration summary for debugging purposes.
     */
    public static String getConfigurationSummary() {
        return String.format(
            "IoUring Configuration:\n" +
            "  Global enabled: %s\n" +
            "  Storage backend: %s (enabled: %s)\n" +
            "  Logging type: %s (enabled: %s)",
            IOURING_ENABLED,
            System.getProperty(STORAGE_BACKEND_PROPERTY, "default"),
            IOURING_STORAGE_ENABLED,
            System.getProperty(LOGGING_TYPE_PROPERTY, "default"),
            IOURING_LOGGING_ENABLED
        );
    }
} 