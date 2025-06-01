package dk.ku.di.dms.vms.modb.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.modb.storage.record.IoUringRecordBufferContext;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoUringStorageFactoryTest {

    private String originalIoUringEnabled;
    private String originalStorageBackend;

    @Before
    public void setUp() {
        // Save original system properties
        originalIoUringEnabled = System.getProperty("iouring.enabled");
        originalStorageBackend = System.getProperty("storage_backend");
        
        // Clear system properties to start with clean state
        System.clearProperty("iouring.enabled");
        System.clearProperty("storage_backend");
    }

    @After
    public void tearDown() {
        // Restore original system properties
        setPropertyOrClear("iouring.enabled", originalIoUringEnabled);
        setPropertyOrClear("storage_backend", originalStorageBackend);
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
        assertFalse(IoUringStorageFactory.isIoUringEnabled());
    }

    @Test
    public void testIsIoUringEnabledTrue() {
        System.setProperty("iouring.enabled", "true");
        assertTrue(IoUringStorageFactory.isIoUringEnabled());
    }

    @Test
    public void testIsIoUringEnabledFalse() {
        System.setProperty("iouring.enabled", "false");
        assertFalse(IoUringStorageFactory.isIoUringEnabled());
    }

    @Test
    public void testIsIoUringStorageEnabledWhenGlobalDisabled() {
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "iouring");
        
        assertFalse(IoUringStorageFactory.isIoUringStorageEnabled());
    }

    @Test
    public void testIsIoUringStorageEnabledWhenGlobalEnabledAndBackendIoUring() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "iouring");
        
        assertTrue(IoUringStorageFactory.isIoUringStorageEnabled());
    }

    @Test
    public void testIsIoUringStorageEnabledWhenGlobalEnabledAndBackendDefault() {
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "default");
        
        assertFalse(IoUringStorageFactory.isIoUringStorageEnabled());
    }

    @Test
    public void testCreateRecordBufferContextFallbackToDefault() {
        // When IoUring is disabled, should fallback to default implementation
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "default");
        
        try (Arena arena = Arena.ofShared()) {
            MemorySegment segment = arena.allocate(1024);
            
            RecordBufferContext context = IoUringStorageFactory.createRecordBufferContext(
                segment, "test.data", 1024);
            
            assertNotNull(context);
            assertTrue(context instanceof RecordBufferContext);
            // Should not be IoUring implementation
            assertFalse(context instanceof IoUringRecordBufferContext);
        }
    }

    @Test
    public void testCreateRecordBufferContextWithIoUringFallback() {
        // When IoUring is enabled but IoUring classes fail to initialize, should fallback
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "iouring");
        
        try (Arena arena = Arena.ofShared()) {
            MemorySegment segment = arena.allocate(1024);
            
            RecordBufferContext context = IoUringStorageFactory.createRecordBufferContext(
                segment, "test.data", 1024);
            
            assertNotNull(context);
            // Should fallback to default implementation if IoUring fails
            assertTrue(context instanceof RecordBufferContext);
        }
    }

    @Test
    public void testCreateRecordBufferContextWithoutFileSize() {
        // Test the overloaded method without file size
        try (Arena arena = Arena.ofShared()) {
            MemorySegment segment = arena.allocate(1024);
            
            RecordBufferContext context = IoUringStorageFactory.createRecordBufferContext(
                segment, "test.data");
            
            assertNotNull(context);
            assertTrue(context instanceof RecordBufferContext);
            assertEquals("test.data", context.fileName);
        }
    }

    @Test
    public void testEnableIoUringStorage() {
        IoUringStorageFactory.enableIoUringStorage();
        
        assertEquals("true", System.getProperty("iouring.enabled"));
        assertEquals("iouring", System.getProperty("storage_backend"));
        
        assertTrue(IoUringStorageFactory.isIoUringEnabled());
        assertTrue(IoUringStorageFactory.isIoUringStorageEnabled());
    }

    @Test
    public void testDisableIoUringStorage() {
        // First enable it
        IoUringStorageFactory.enableIoUringStorage();
        assertTrue(IoUringStorageFactory.isIoUringStorageEnabled());
        
        // Then disable it
        IoUringStorageFactory.disableIoUringStorage();
        
        assertEquals("default", System.getProperty("storage_backend"));
        assertFalse(IoUringStorageFactory.isIoUringStorageEnabled());
    }

    @Test
    public void testMemorySegmentAddress() {
        // Test that the created context preserves memory segment properties
        try (Arena arena = Arena.ofShared()) {
            MemorySegment segment = arena.allocate(1024);
            long originalAddress = segment.address();
            
            RecordBufferContext context = IoUringStorageFactory.createRecordBufferContext(
                segment, "test.data", 1024);
            
            assertEquals(originalAddress, context.address);
            assertEquals("test.data", context.fileName);
        }
    }

    @Test
    public void testCreateWithNullFileName() {
        // Test creating context with null file name
        try (Arena arena = Arena.ofShared()) {
            MemorySegment segment = arena.allocate(1024);
            
            RecordBufferContext context = IoUringStorageFactory.createRecordBufferContext(
                segment, null, 1024);
            
            assertNotNull(context);
            assertNull(context.fileName);
        }
    }

    @Test
    public void testCreateWithZeroFileSize() {
        // Test creating context with zero file size
        try (Arena arena = Arena.ofShared()) {
            MemorySegment segment = arena.allocate(1024);
            
            RecordBufferContext context = IoUringStorageFactory.createRecordBufferContext(
                segment, "test.data", 0);
            
            assertNotNull(context);
            assertEquals("test.data", context.fileName);
        }
    }

    @Test
    public void testSequentialEnableDisable() {
        // Test enabling and disabling storage multiple times
        assertFalse(IoUringStorageFactory.isIoUringStorageEnabled());
        
        IoUringStorageFactory.enableIoUringStorage();
        assertTrue(IoUringStorageFactory.isIoUringStorageEnabled());
        
        IoUringStorageFactory.disableIoUringStorage();
        assertFalse(IoUringStorageFactory.isIoUringStorageEnabled());
        
        IoUringStorageFactory.enableIoUringStorage();
        assertTrue(IoUringStorageFactory.isIoUringStorageEnabled());
    }
} 