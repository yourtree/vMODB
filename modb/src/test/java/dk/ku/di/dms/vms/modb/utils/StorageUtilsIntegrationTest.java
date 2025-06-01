package dk.ku.di.dms.vms.modb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IntKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.storage.record.IoUringRecordBufferContext;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StorageUtilsIntegrationTest {

    private String originalIoUringEnabled;
    private String originalStorageBackend;
    private String originalUserHome;
    
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Before
    public void setUp() {
        // Save original system properties
        originalIoUringEnabled = System.getProperty("iouring.enabled");
        originalStorageBackend = System.getProperty("storage_backend");
        originalUserHome = System.getProperty("user.home");
        
        // Clear system properties to start with clean state
        System.clearProperty("iouring.enabled");
        System.clearProperty("storage_backend");
        
        // Set temporary user home for testing
        System.setProperty("user.home", tempDir.getRoot().toString());
    }

    @After
    public void tearDown() {
        // Restore original system properties
        setPropertyOrClear("iouring.enabled", originalIoUringEnabled);
        setPropertyOrClear("storage_backend", originalStorageBackend);
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
    public void testLoadRecordBufferWithDefaultBackend() {
        // Test loading record buffer with default backend
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "default");
        
        RecordBufferContext context = StorageUtils.loadRecordBuffer(
            10, 100, "test_default", true);
        
        assertNotNull(context);
        assertTrue(context instanceof RecordBufferContext);
        assertFalse(context instanceof IoUringRecordBufferContext);
        assertEquals("test_default", context.fileName);
    }

    @Test
    public void testLoadRecordBufferWithIoUringBackendFallback() {
        // Test loading record buffer with IoUring backend (should fallback if not available)
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "iouring");
        
        RecordBufferContext context = StorageUtils.loadRecordBuffer(
            10, 100, "test_iouring", true);
        
        assertNotNull(context);
        assertTrue(context instanceof RecordBufferContext);
        assertEquals("test_iouring", context.fileName);
        // Note: May or may not be IoUring depending on whether native libraries are available
    }

    @Test
    public void testCreatePrimaryIndexWithCheckpointing() {
        // Test creating primary index with checkpointing enabled
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "default");
        
        Schema schema = new Schema(
            new String[]{"id", "name"}, 
            new DataType[]{DataType.INT, DataType.STRING}, 
            new int[]{0}, 
            null, 
            false
        );
        
        PrimaryIndex index = StorageUtils.createPrimaryIndex(
            "test_table", schema, true, true, false, 100);
        
        assertNotNull(index);
        assertNotNull(index.underlyingIndex());
        assertTrue(index.underlyingIndex() instanceof UniqueHashBufferIndex);
    }

    @Test
    public void testCreatePrimaryIndexWithoutCheckpointing() {
        // Test creating primary index without checkpointing (in-memory)
        Schema schema = new Schema(
            new String[]{"id", "name"}, 
            new DataType[]{DataType.INT, DataType.STRING}, 
            new int[]{0}, 
            null, 
            false
        );
        
        PrimaryIndex index = StorageUtils.createPrimaryIndex(
            "test_table", schema, false, false, false, 100);
        
        assertNotNull(index);
        assertNotNull(index.underlyingIndex());
        // Should be in-memory implementation when checkpointing is disabled
    }

    @Test
    public void testCreatePrimaryIndexWithIoUring() {
        // Test creating primary index with IoUring enabled
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "iouring");
        
        Schema schema = new Schema(
            new String[]{"id", "name"}, 
            new DataType[]{DataType.INT, DataType.STRING}, 
            new int[]{0}, 
            null, 
            false
        );
        
        PrimaryIndex index = StorageUtils.createPrimaryIndex(
            "test_table_iouring", schema, true, true, false, 100);
        
        assertNotNull(index);
        assertNotNull(index.underlyingIndex());
        assertTrue(index.underlyingIndex() instanceof UniqueHashBufferIndex);
    }

    @Test
    public void testRecordBufferContextFunctionality() throws IOException {
        // Test that the created record buffer context works correctly
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "default");
        
        RecordBufferContext context = StorageUtils.loadRecordBuffer(
            10, 100, "test_functionality", true);
        
        assertNotNull(context);
        assertTrue(context.address > 0);
        assertEquals("test_functionality", context.fileName);
        
        // Test force operation
        try {
            context.force();
        } catch (Exception e) {
            assertTrue("Context force should not throw exception", false);
        }
    }

    @Test
    public void testIndexOperationsWithStorageUtils() {
        // Test actual index operations with storage utils
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "default");
        
        Schema schema = new Schema(
            new String[]{"id", "value"}, 
            new DataType[]{DataType.INT, DataType.INT}, 
            new int[]{0}, 
            null, 
            false
        );
        
        PrimaryIndex index = StorageUtils.createPrimaryIndex(
            "test_ops", schema, true, true, false, 100);
        
        // Test basic operations
        UniqueHashBufferIndex bufferIndex = (UniqueHashBufferIndex) index.underlyingIndex();
        
        // Insert some test data
        Object[] record1 = new Object[]{1, 100};
        Object[] record2 = new Object[]{2, 200};
        
        bufferIndex.insert(IntKey.of(1), record1);
        bufferIndex.insert(IntKey.of(2), record2);
        
        // Verify data
        assertTrue(bufferIndex.exists(IntKey.of(1)));
        assertTrue(bufferIndex.exists(IntKey.of(2)));
        assertFalse(bufferIndex.exists(IntKey.of(3)));
        
        Object[] retrieved1 = bufferIndex.lookupByKey(IntKey.of(1));
        assertNotNull(retrieved1);
        assertEquals(1, retrieved1[0]);
        assertEquals(100, retrieved1[1]);
        
        // Test flush operation
        try {
            bufferIndex.flush();
        } catch (Exception e) {
            assertTrue("Buffer flush should not throw exception", false);
        }
    }

    @Test
    public void testFileCreationAndPath() throws IOException {
        // Test that files are created in the correct location
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "default");
        
        String fileName = "test_file_creation";
        StorageUtils.loadRecordBuffer(10, 100, fileName, true);
        
        // Check that VMS directory was created
        Path vmsDir = tempDir.getRoot().toPath().resolve("vms");
        assertTrue(Files.exists(vmsDir));
        assertTrue(Files.isDirectory(vmsDir));
        
        // Check that data file was created
        Path dataFile = vmsDir.resolve(fileName + ".data");
        assertTrue(Files.exists(dataFile));
        assertTrue(Files.isRegularFile(dataFile));
    }

    @Test
    public void testGetBasePath() {
        // Test base path generation
        String basePath = StorageUtils.getBasePath();
        
        assertNotNull(basePath);
        assertTrue(basePath.contains(tempDir.getRoot().toString()));
        assertTrue(basePath.endsWith("/vms/") || basePath.endsWith("\\vms\\"));
    }

    @Test
    public void testBuildFile() {
        // Test file building
        String fileName = "test_build_file";
        java.io.File file = StorageUtils.buildFile(fileName);
        
        assertNotNull(file);
        assertTrue(file.getAbsolutePath().contains("vms"));
        assertTrue(file.getAbsolutePath().contains(fileName));
        assertTrue(file.getAbsolutePath().endsWith(".data"));
        
        // Test that parent directory is created
        assertTrue(file.getParentFile().exists());
    }

    @Test
    public void testMultipleRecordBufferContexts() {
        // Test creating multiple record buffer contexts
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "default");
        
        RecordBufferContext context1 = StorageUtils.loadRecordBuffer(
            10, 100, "test_multiple_1", true);
        RecordBufferContext context2 = StorageUtils.loadRecordBuffer(
            20, 200, "test_multiple_2", true);
        
        assertNotNull(context1);
        assertNotNull(context2);
        assertNotEquals(context1.address, context2.address);
        assertNotEquals(context1.fileName, context2.fileName);
    }

    @Test
    public void testRecordBufferContextWithTruncate() {
        // Test record buffer context creation with truncate option
        System.setProperty("iouring.enabled", "false");
        System.setProperty("storage_backend", "default");
        
        // Create with truncate
        RecordBufferContext context1 = StorageUtils.loadRecordBuffer(
            10, 100, "test_truncate", true);
        assertNotNull(context1);
        
        // Create again without truncate
        RecordBufferContext context2 = StorageUtils.loadRecordBuffer(
            10, 100, "test_truncate", false);
        assertNotNull(context2);
    }
} 