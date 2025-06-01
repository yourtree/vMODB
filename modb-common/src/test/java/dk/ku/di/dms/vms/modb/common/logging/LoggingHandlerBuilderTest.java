package dk.ku.di.dms.vms.modb.common.logging;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LoggingHandlerBuilderTest {

    private String originalIoUringEnabled;
    private String originalLoggingType;
    private String originalUserHome;
    
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Before
    public void setUp() {
        // Save original system properties
        originalIoUringEnabled = System.getProperty("iouring.enabled");
        originalLoggingType = System.getProperty("logging_type");
        originalUserHome = System.getProperty("user.home");
        
        // Clear system properties to start with clean state
        System.clearProperty("iouring.enabled");
        System.clearProperty("logging_type");
        
        // Set temporary user home for testing
        System.setProperty("user.home", tempDir.getRoot().toString());
    }

    @After
    public void tearDown() {
        // Restore original system properties
        setPropertyOrClear("iouring.enabled", originalIoUringEnabled);
        setPropertyOrClear("logging_type", originalLoggingType);
        setPropertyOrClear("user.home", originalUserHome);
    }

    private void setPropertyOrClear(String key, String value) {
        if (value != null) {
            System.setProperty(key, value);
        } else {
            System.clearProperty(key);
        }
    }

    @Test
    public void testBuildDefaultLoggingHandler() {
        // When no properties are set, should create DefaultLoggingHandler
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        ILoggingHandler handler = LoggingHandlerBuilder.build("test");
        
        assertNotNull(handler);
        assertTrue(handler instanceof DefaultLoggingHandler);
        assertTrue(handler.getFileName().contains("test"));
        assertTrue(handler.getFileName().endsWith(".llog"));
    }

    @Test
    public void testBuildCompressedLoggingHandler() {
        // When compressed logging is requested, should create CompressedLoggingHandler
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "compressed");
        
        ILoggingHandler handler = LoggingHandlerBuilder.build("test");
        
        assertNotNull(handler);
        assertTrue(handler instanceof CompressedLoggingHandler);
        assertTrue(handler.getFileName().contains("test"));
    }

    @Test
    public void testBuildWithIoUringFallback() {
        // When IoUring is requested but not available, should fallback
        System.setProperty("iouring.enabled", "true");
        System.setProperty("logging_type", "iouring");
        
        ILoggingHandler handler = LoggingHandlerBuilder.build("test");
        
        assertNotNull(handler);
        // Should fallback to default implementation if IoUring is not available
        assertTrue(handler instanceof DefaultLoggingHandler || 
                  handler instanceof IoUringLoggingHandler);
    }

    @Test
    public void testBuildWithCompressedIoUringFallback() {
        // When compressed IoUring is requested but not available, should fallback
        System.setProperty("iouring.enabled", "true");
        System.setProperty("logging_type", "compressed_iouring");
        
        ILoggingHandler handler = LoggingHandlerBuilder.build("test");
        
        assertNotNull(handler);
        // Should fallback if IoUring is not available
        assertNotNull(handler);
    }

    @Test
    public void testBuildCreatesVmsDirectory() throws IOException {
        // Test that the VMS directory is created
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        ILoggingHandler handler = LoggingHandlerBuilder.build("test");
        
        assertNotNull(handler);
        
        // Check that vms directory was created
        Path vmsDir = tempDir.getRoot().toPath().resolve("vms");
        assertTrue(Files.exists(vmsDir));
        assertTrue(Files.isDirectory(vmsDir));
    }

    @Test
    public void testBuildWithUniqueTimestamp() {
        // Test that each build creates a handler with unique filename (timestamp based)
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        ILoggingHandler handler1 = LoggingHandlerBuilder.build("test");
        
        // Small delay to ensure different timestamp
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        ILoggingHandler handler2 = LoggingHandlerBuilder.build("test");
        
        assertNotNull(handler1);
        assertNotNull(handler2);
        assertNotEquals(handler1.getFileName(), handler2.getFileName());
    }

    @Test
    public void testBuildWithDifferentIdentifiers() {
        // Test that different identifiers create different handlers
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        ILoggingHandler handler1 = LoggingHandlerBuilder.build("test1");
        ILoggingHandler handler2 = LoggingHandlerBuilder.build("test2");
        
        assertNotNull(handler1);
        assertNotNull(handler2);
        assertTrue(handler1.getFileName().contains("test1"));
        assertTrue(handler2.getFileName().contains("test2"));
    }

    @Test
    public void testBuildHandlerCanBeUsed() throws IOException {
        // Test that the created handler can actually be used for logging
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        ILoggingHandler handler = LoggingHandlerBuilder.build("test");
        
        assertNotNull(handler);
        
        // Test basic operations
        try {
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap("test message".getBytes());
            handler.log(buffer);
            handler.force();
            handler.close();
        } catch (Exception e) {
            // Should not throw exception
            fail("Handler should work without throwing exception");
        }
    }

    @Test
    public void testBuildWithNullIdentifier() {
        // Test building with null identifier
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        // Should handle null identifier gracefully
        try {
            ILoggingHandler handler = LoggingHandlerBuilder.build(null);
            assertNotNull(handler);
        } catch (Exception e) {
            // Should not throw exception for null identifier
            fail("Should handle null identifier gracefully");
        }
    }

    @Test
    public void testBuildWithEmptyIdentifier() {
        // Test building with empty identifier
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        ILoggingHandler handler = LoggingHandlerBuilder.build("");
        
        assertNotNull(handler);
        assertNotNull(handler.getFileName());
    }

    @Test
    public void testBuildWithSpecialCharactersInIdentifier() {
        // Test building with special characters in identifier
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        ILoggingHandler handler = LoggingHandlerBuilder.build("test-with_special.chars");
        
        assertNotNull(handler);
        assertTrue(handler.getFileName().contains("test-with_special.chars"));
    }

    @Test
    public void testMultipleBuildCallsWithSameIdentifier() {
        // Test that multiple build calls with same identifier work
        System.setProperty("iouring.enabled", "false");
        System.setProperty("logging_type", "default");
        
        ILoggingHandler handler1 = LoggingHandlerBuilder.build("test");
        ILoggingHandler handler2 = LoggingHandlerBuilder.build("test");
        
        assertNotNull(handler1);
        assertNotNull(handler2);
        // Should have different timestamps in filename
        assertNotEquals(handler1.getFileName(), handler2.getFileName());
    }
} 