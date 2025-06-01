package dk.ku.di.dms.vms.modb.common.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoUringConfigurationFactoryTest {

    private String originalIoUringEnabled;
    private String originalStorageBackend;
    private String originalLoggingType;

    @Before
    public void setUp() {
        // Save original system properties
        originalIoUringEnabled = System.getProperty("iouring.enabled");
        originalStorageBackend = System.getProperty("storage_backend");
        originalLoggingType = System.getProperty("logging_type");
        
        // Clear system properties to start with clean state
        System.clearProperty("iouring.enabled");
        System.clearProperty("storage_backend");
        System.clearProperty("logging_type");
    }

    @After
    public void tearDown() {
        // Restore original system properties
        setPropertyOrClear("iouring.enabled", originalIoUringEnabled);
        setPropertyOrClear("storage_backend", originalStorageBackend);
        setPropertyOrClear("logging_type", originalLoggingType);
    }

    private void setPropertyOrClear(String key, String value) {
        if (value != null) {
            System.setProperty(key, value);
        } else {
            System.clearProperty(key);
        }
    }

    @Test
    public void testIsIoUringEnabledDefault() {
        // When no property is set, should default to false
        assertFalse(IoUringConfigurationFactory.isIoUringEnabled());
    }

    @Test
    public void testIsIoUringEnabledTrue() {
        System.setProperty("iouring.enabled", "true");
        assertTrue(IoUringConfigurationFactory.isIoUringEnabled());
    }

    @Test
    public void testIsIoUringEnabledFalse() {
        System.setProperty("iouring.enabled", "false");
        assertFalse(IoUringConfigurationFactory.isIoUringEnabled());
    }

    @Test
    public void testIsIoUringStorageEnabledWhenGlobalDisabled() {
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "iouring");
        
        assertFalse(IoUringConfigurationFactory.isIoUringStorageEnabled());
    }

    @Test
    public void testIsIoUringStorageEnabledWhenGlobalEnabledAndBackendIoUring() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "iouring");
        
        assertTrue(IoUringConfigurationFactory.isIoUringStorageEnabled());
    }

    @Test
    public void testIsIoUringStorageEnabledWhenGlobalEnabledAndBackendDefault() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "default");
        
        assertFalse(IoUringConfigurationFactory.isIoUringStorageEnabled());
    }

    @Test
    public void testIsIoUringLoggingEnabledWhenGlobalDisabled() {
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "iouring");
        
        assertFalse(IoUringConfigurationFactory.isIoUringLoggingEnabled());
    }

    @Test
    public void testIsIoUringLoggingEnabledWithIoUringType() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("logging_type", "iouring");
        
        assertTrue(IoUringConfigurationFactory.isIoUringLoggingEnabled());
    }

    @Test
    public void testIsIoUringLoggingEnabledWithCompressedIoUringType() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("logging_type", "compressed_iouring");
        
        assertTrue(IoUringConfigurationFactory.isIoUringLoggingEnabled());
    }

    @Test
    public void testIsIoUringLoggingEnabledWithDefaultType() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("logging_type", "default");
        
        assertFalse(IoUringConfigurationFactory.isIoUringLoggingEnabled());
    }

    @Test
    public void testCreateLoggingHandlerWithDefaultConfig() {
        // When IoUring is disabled, should create standard handlers
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        ILoggingHandler handler = IoUringConfigurationFactory.createLoggingHandler("test");
        
        assertNotNull(handler);
        assertTrue(handler.getFileName().contains("test"));
        assertTrue(handler.getFileName().endsWith(".llog"));
    }

    @Test
    public void testCreateLoggingHandlerWithCompressed() {
        // When compressed logging is requested
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "compressed");
        
        ILoggingHandler handler = IoUringConfigurationFactory.createLoggingHandler("test");
        
        assertNotNull(handler);
        assertTrue(handler.getFileName().contains("test"));
    }

    @Test
    public void testCreateLoggingHandlerWithIoUringFallback() {
        // When IoUring is enabled but IoUring classes are not available, should fallback
        System.setProperty("iouring.enabled", "true");
        System.setProperty("logging_type", "iouring");
        
        ILoggingHandler handler = IoUringConfigurationFactory.createLoggingHandler("test");
        
        // Should fallback to default since IoUring classes might not be available in test environment
        assertNotNull(handler);
    }

    @Test
    public void testEnableIoUring() {
        IoUringConfigurationFactory.enableIoUring();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("iouring", System.getProperty("storage_backend"));
        assertEquals("iouring", System.getProperty("logging_type"));
    }

    @Test
    public void testEnableIoUringWithCompression() {
        IoUringConfigurationFactory.enableIoUringWithCompression();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("iouring", System.getProperty("storage_backend"));
        assertEquals("compressed_iouring", System.getProperty("logging_type"));
    }

    @Test
    public void testDisableIoUring() {
        // First enable it
        IoUringConfigurationFactory.enableIoUring();
        
        // Then disable it
        IoUringConfigurationFactory.disableIoUring();
        
        assertEquals("false", System.getProperty("iouring.enabled"));
        assertEquals("default", System.getProperty("storage_backend"));
        assertEquals("default", System.getProperty("logging_type"));
    }

    @Test
    public void testGetConfigurationSummary() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "iouring");
        System.setProperty("logging_type", "compressed_iouring");
        
        String summary = IoUringConfigurationFactory.getConfigurationSummary();
        
        assertNotNull(summary);
        assertTrue(summary.contains("IoUring Configuration:"));
        assertTrue(summary.contains("Global enabled: true"));
        assertTrue(summary.contains("Storage backend: iouring"));
        assertTrue(summary.contains("Logging type: compressed_iouring"));
    }

    @Test
    public void testGetConfigurationSummaryWithDefaults() {
        // Test with default values
        String summary = IoUringConfigurationFactory.getConfigurationSummary();
        
        assertNotNull(summary);
        assertTrue(summary.contains("IoUring Configuration:"));
        assertTrue(summary.contains("Global enabled: false"));
        assertTrue(summary.contains("Storage backend: default"));
        assertTrue(summary.contains("Logging type: default"));
    }
} 