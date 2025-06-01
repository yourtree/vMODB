package dk.ku.di.dms.vms.modb.common.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoUringBootstrapTest {

    private String originalIoUringEnabled;
    private String originalStorageBackend;
    private String originalLoggingType;
    private String originalEnableByDefault;
    private String originalCompressionEnabled;
    
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    private final PrintStream standardOut = System.out;

    @Before
    public void setUp() {
        // Save original system properties
        originalIoUringEnabled = System.getProperty("iouring.enabled");
        originalStorageBackend = System.getProperty("storage_backend");
        originalLoggingType = System.getProperty("logging_type");
        originalEnableByDefault = System.getProperty("iouring.enable_by_default");
        originalCompressionEnabled = System.getProperty("iouring.compression_enabled");
        
        // Clear system properties to start with clean state
        System.clearProperty("iouring.enabled");
        System.clearProperty("storage_backend");
        System.clearProperty("logging_type");
        System.clearProperty("iouring.enable_by_default");
        System.clearProperty("iouring.compression_enabled");
        
        // Capture System.out for testing print methods
        System.setOut(new PrintStream(outputStreamCaptor));
    }

    @After
    public void tearDown() {
        // Restore original system properties
        setPropertyOrClear("iouring.enabled", originalIoUringEnabled);
        setPropertyOrClear("storage_backend", originalStorageBackend);
        setPropertyOrClear("logging_type", originalLoggingType);
        setPropertyOrClear("iouring.enable_by_default", originalEnableByDefault);
        setPropertyOrClear("iouring.compression_enabled", originalCompressionEnabled);
        
        // Restore System.out
        System.setOut(standardOut);
    }

    private void setPropertyOrClear(String key, String value) {
        if (value != null) {
            System.setProperty(key, value);
        } else {
            System.clearProperty(key);
        }
    }

    @Test
    public void testEnableAll() {
        IoUringBootstrap.enableAll();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("iouring", System.getProperty("storage_backend"));
        assertEquals("iouring", System.getProperty("logging_type"));
        
        // Verify that status was printed
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
        assertTrue(output.contains("Global enabled: true"));
    }

    @Test
    public void testEnableAllWithCompression() {
        IoUringBootstrap.enableAllWithCompression();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("iouring", System.getProperty("storage_backend"));
        assertEquals("compressed_iouring", System.getProperty("logging_type"));
        
        // Verify that status was printed
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
        assertTrue(output.contains("compressed_iouring"));
    }

    @Test
    public void testEnableStorageOnly() {
        IoUringBootstrap.enableStorageOnly();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("iouring", System.getProperty("storage_backend"));
        assertEquals("default", System.getProperty("logging_type"));
        
        // Verify that status was printed
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
    }

    @Test
    public void testEnableLoggingOnly() {
        IoUringBootstrap.enableLoggingOnly();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("default", System.getProperty("storage_backend"));
        assertEquals("iouring", System.getProperty("logging_type"));
        
        // Verify that status was printed
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
    }

    @Test
    public void testEnableCompressedLoggingOnly() {
        IoUringBootstrap.enableCompressedLoggingOnly();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("default", System.getProperty("storage_backend"));
        assertEquals("compressed_iouring", System.getProperty("logging_type"));
        
        // Verify that status was printed
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
    }

    @Test
    public void testDisableAll() {
        // First enable something
        IoUringBootstrap.enableAll();
        outputStreamCaptor.reset(); // Clear previous output
        
        // Then disable
        IoUringBootstrap.disableAll();
        
        assertEquals("false", System.getProperty("iouring.enabled"));
        assertEquals("default", System.getProperty("storage_backend"));
        assertEquals("default", System.getProperty("logging_type"));
        
        // Verify that status was printed
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
        assertTrue(output.contains("Global enabled: false"));
    }

    @Test
    public void testPrintStatus() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "iouring");
        System.setProperty("logging_type", "compressed_iouring");
        
        IoUringBootstrap.printStatus();
        
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
        assertTrue(output.contains("IoUring Configuration:"));
        assertTrue(output.contains("Global enabled: true"));
        assertTrue(output.contains("Storage backend: iouring"));
        assertTrue(output.contains("Logging type: compressed_iouring"));
        assertTrue(output.contains("=============================="));
    }

    @Test
    public void testInitializeWithoutDefaults() {
        // When enable_by_default is false or not set, should not enable IoUring
        IoUringBootstrap.initialize();
        
        // Should not change any properties (they remain null/cleared)
        assertNull(System.getProperty("iouring.enabled"));
        
        // But should still print status
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
    }

    @Test
    public void testInitializeWithDefaultEnabled() {
        System.setProperty("iouring.enable_by_default", "true");
        System.setProperty("iouring.compression_enabled", "false");
        
        IoUringBootstrap.initialize();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("iouring", System.getProperty("storage_backend"));
        assertEquals("iouring", System.getProperty("logging_type"));
        
        // Verify that status was printed
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
    }

    @Test
    public void testInitializeWithDefaultEnabledAndCompression() {
        System.setProperty("iouring.enable_by_default", "true");
        System.setProperty("iouring.compression_enabled", "true");
        
        IoUringBootstrap.initialize();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("iouring", System.getProperty("storage_backend"));
        assertEquals("compressed_iouring", System.getProperty("logging_type"));
        
        // Verify that status was printed
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
    }

    @Test
    public void testMultipleEnableCalls() {
        // Test that multiple enable calls work correctly
        IoUringBootstrap.enableStorageOnly();
        outputStreamCaptor.reset();
        
        IoUringBootstrap.enableLoggingOnly();
        
        // Should have logging enabled but storage disabled
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("default", System.getProperty("storage_backend"));
        assertEquals("iouring", System.getProperty("logging_type"));
    }

    @Test
    public void testSequentialEnableDisable() {
        // Test enabling and then disabling
        IoUringBootstrap.enableAll();
        assertTrue(IoUringConfigurationFactory.isIoUringEnabled());
        
        outputStreamCaptor.reset();
        IoUringBootstrap.disableAll();
        assertFalse(IoUringConfigurationFactory.isIoUringEnabled());
        
        // Verify disable was properly printed
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("Global enabled: false"));
    }
} 