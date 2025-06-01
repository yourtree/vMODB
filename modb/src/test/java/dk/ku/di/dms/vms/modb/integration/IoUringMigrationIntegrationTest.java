package dk.ku.di.dms.vms.modb.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.modb.common.config.IoUringBootstrap;
import dk.ku.di.dms.vms.modb.common.config.IoUringConfigurationFactory;
import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionManager;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;
import dk.ku.di.dms.vms.modb.utils.StorageUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end integration test for IoUring migration.
 * Tests the complete integration from configuration to storage and logging.
 */
public class IoUringMigrationIntegrationTest {

    private String originalIoUringEnabled;
    private String originalStorageBackend;
    private String originalLoggingType;
    private String originalUserHome;
    
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    private final PrintStream standardOut = System.out;

    @Before
    public void setUp() {
        // Save original system properties
        originalIoUringEnabled = System.getProperty("iouring.enabled");
        originalStorageBackend = System.getProperty("storage_backend");
        originalLoggingType = System.getProperty("logging_type");
        originalUserHome = System.getProperty("user.home");
        
        // Clear system properties to start with clean state
        System.clearProperty("iouring.enabled");
        System.clearProperty("storage_backend");
        System.clearProperty("logging_type");
        
        // Set temporary user home for testing
        System.setProperty("user.home", tempDir.getRoot().toString());
        
        // Capture System.out for testing
        System.setOut(new PrintStream(outputStreamCaptor));
    }

    @After
    public void tearDown() {
        // Restore original system properties
        setPropertyOrClear("iouring.enabled", originalIoUringEnabled);
        setPropertyOrClear("storage_backend", originalStorageBackend);
        setPropertyOrClear("logging_type", originalLoggingType);
        setPropertyOrClear("user.home", originalUserHome);
        
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
    public void testCompleteDefaultConfiguration() {
        // Test complete system with default configuration
        outputStreamCaptor.reset();
        
        // This should use default implementations
        IoUringBootstrap.disableAll();
        
        // Verify configuration
        assertFalse(IoUringConfigurationFactory.isIoUringEnabled());
        assertFalse(IoUringConfigurationFactory.isIoUringStorageEnabled());
        assertFalse(IoUringConfigurationFactory.isIoUringLoggingEnabled());
        
        // Test storage creation
        RecordBufferContext context = StorageUtils.loadRecordBuffer(10, 100, "test_default", true);
        assertNotNull(context);
        
        // Test logging creation
        ILoggingHandler logger = LoggingHandlerBuilder.build("test_default");
        assertNotNull(logger);
        
        // Verify output contains status
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("Global enabled: false"));
    }

    @Test
    public void testCompleteIoUringConfiguration() {
        // Test complete system with IoUring configuration
        outputStreamCaptor.reset();
        
        // Enable IoUring
        IoUringBootstrap.enableAll();
        
        // Verify configuration
        assertTrue(IoUringConfigurationFactory.isIoUringEnabled());
        assertTrue(IoUringConfigurationFactory.isIoUringStorageEnabled());
        assertTrue(IoUringConfigurationFactory.isIoUringLoggingEnabled());
        
        // Test storage creation (should fallback if IoUring not available)
        RecordBufferContext context = StorageUtils.loadRecordBuffer(10, 100, "test_iouring", true);
        assertNotNull(context);
        assertEquals("test_iouring", context.fileName);
        
        // Test logging creation
        ILoggingHandler logger = LoggingHandlerBuilder.build("test_iouring");
        assertNotNull(logger);
        
        // Verify output contains status
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("Global enabled: true"));
        assertTrue(output.contains("Storage backend: iouring"));
        assertTrue(output.contains("Logging type: iouring"));
    }

    @Test
    public void testMixedConfiguration() {
        // Test mixed configuration (storage only)
        outputStreamCaptor.reset();
        
        IoUringBootstrap.enableStorageOnly();
        
        // Verify mixed configuration
        assertTrue(IoUringConfigurationFactory.isIoUringEnabled());
        assertTrue(IoUringConfigurationFactory.isIoUringStorageEnabled());
        assertFalse(IoUringConfigurationFactory.isIoUringLoggingEnabled());
        
        // Test components work with mixed configuration
        RecordBufferContext context = StorageUtils.loadRecordBuffer(10, 100, "test_mixed", true);
        assertNotNull(context);
        
        ILoggingHandler logger = LoggingHandlerBuilder.build("test_mixed");
        assertNotNull(logger);
        
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("Storage backend: iouring"));
        assertTrue(output.contains("Logging type: default"));
    }

    @Test
    public void testTransactionManagerIntegration() throws IOException {
        // Test TransactionManager with IoUring configuration
        IoUringBootstrap.enableAll();
        
        // Create test schema and table
        Schema schema = new Schema(
            new String[]{"id", "name", "value"}, 
            new DataType[]{DataType.INT, DataType.STRING, DataType.INT}, 
            new int[]{0}, 
            null, 
            false
        );
        
        PrimaryIndex primaryIndex = StorageUtils.createPrimaryIndex(
            "test_table", schema, true, false, false, 100);
        Table table = new Table("test_table", schema, primaryIndex);
        Map<String, Table> catalog = new HashMap<>();
        catalog.put("test_table", table);
        
        // Create TransactionManager (should use IoUring components transparently)
        TransactionManager txManager = new TransactionManager(catalog, true);
        assertNotNull(txManager);
        
        // Test basic operations
        long tid = 1L;
        txManager.beginTransaction(tid, 1, 0L, false);
        
        // Insert test data
        Object[] record1 = new Object[]{1, "test1", 100};
        Object[] record2 = new Object[]{2, "test2", 200};
        
        txManager.insert(table, record1);
        txManager.insert(table, record2);
        
        // Verify data
        assertTrue(txManager.exists(table.primaryKeyIndex(), new Object[]{1}));
        assertTrue(txManager.exists(table.primaryKeyIndex(), new Object[]{2}));
        
        Object[] retrieved = txManager.lookupByKey(table.primaryKeyIndex(), new Object[]{1});
        assertNotNull(retrieved);
        assertEquals(1, retrieved[0]);
        assertEquals("test1", retrieved[1]);
        assertEquals(100, retrieved[2]);
        
        // Test commit and checkpoint
        try {
            txManager.commit();
            txManager.checkpoint(tid);
        } catch (Exception e) {
            assertTrue("Transaction operations should not throw exception", false);
        }
    }

    @Test
    public void testLoggingIntegration() throws IOException {
        // Test logging integration with different configurations
        
        // Test default logging
        IoUringBootstrap.disableAll();
        ILoggingHandler defaultLogger = LoggingHandlerBuilder.build("test_default_log");
        testLoggerFunctionality(defaultLogger);
        
        outputStreamCaptor.reset();
        
        // Test IoUring logging (with fallback)
        IoUringBootstrap.enableLoggingOnly();
        ILoggingHandler ioUringLogger = LoggingHandlerBuilder.build("test_iouring_log");
        testLoggerFunctionality(ioUringLogger);
        
        // Verify configuration was applied
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("Logging type: iouring"));
    }

    private void testLoggerFunctionality(ILoggingHandler logger) throws IOException {
        assertNotNull(logger);
        
        // Test logging operations
        ByteBuffer buffer1 = ByteBuffer.wrap("Test log message 1".getBytes());
        ByteBuffer buffer2 = ByteBuffer.wrap("Test log message 2".getBytes());
        
        try {
            logger.log(buffer1);
            logger.log(buffer2);
            logger.force();
            logger.close();
        } catch (Exception e) {
            assertTrue("Logger should work without throwing exception", false);
        }
    }

    @Test
    public void testConfigurationTransitions() {
        // Test transitions between different configurations
        outputStreamCaptor.reset();
        
        // Start disabled
        IoUringBootstrap.disableAll();
        assertFalse(IoUringConfigurationFactory.isIoUringEnabled());
        
        // Enable all
        IoUringBootstrap.enableAll();
        assertTrue(IoUringConfigurationFactory.isIoUringEnabled());
        assertTrue(IoUringConfigurationFactory.isIoUringStorageEnabled());
        assertTrue(IoUringConfigurationFactory.isIoUringLoggingEnabled());
        
        // Enable storage only
        IoUringBootstrap.enableStorageOnly();
        assertTrue(IoUringConfigurationFactory.isIoUringEnabled());
        assertTrue(IoUringConfigurationFactory.isIoUringStorageEnabled());
        assertFalse(IoUringConfigurationFactory.isIoUringLoggingEnabled());
        
        // Enable logging only
        IoUringBootstrap.enableLoggingOnly();
        assertTrue(IoUringConfigurationFactory.isIoUringEnabled());
        assertFalse(IoUringConfigurationFactory.isIoUringStorageEnabled());
        assertTrue(IoUringConfigurationFactory.isIoUringLoggingEnabled());
        
        // Back to disabled
        IoUringBootstrap.disableAll();
        assertFalse(IoUringConfigurationFactory.isIoUringEnabled());
        
        // Verify all transitions were logged
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("=== IoUring Bootstrap Status ==="));
    }

    @Test
    public void testSystemPropertiesIntegration() {
        // Test integration with system properties directly
        
        // Set properties manually
        System.setProperty("iouring.enabled", "true");
        System.setProperty("storage_backend", "iouring");
        System.setProperty("logging_type", "compressed_iouring");
        
        // Verify factory reads them correctly
        assertTrue(IoUringConfigurationFactory.isIoUringEnabled());
        assertTrue(IoUringConfigurationFactory.isIoUringStorageEnabled());
        assertTrue(IoUringConfigurationFactory.isIoUringLoggingEnabled());
        
        // Test components use the configuration
        RecordBufferContext context = StorageUtils.loadRecordBuffer(10, 100, "test_props", true);
        assertNotNull(context);
        
        ILoggingHandler logger = LoggingHandlerBuilder.build("test_props");
        assertNotNull(logger);
    }

    @Test
    public void testFallbackMechanisms() {
        // Test that fallback mechanisms work correctly
        
        // Enable IoUring but expect fallback due to missing native libraries
        IoUringBootstrap.enableAll();
        
        // Create components - should fallback gracefully
        RecordBufferContext context = StorageUtils.loadRecordBuffer(10, 100, "test_fallback", true);
        assertNotNull(context);
        assertEquals("test_fallback", context.fileName);
        
        ILoggingHandler logger = LoggingHandlerBuilder.build("test_fallback");
        assertNotNull(logger);
        
        // Should work despite potential IoUring unavailability
        try {
            context.force();
            ByteBuffer buffer = ByteBuffer.wrap("fallback test".getBytes());
            logger.log(buffer);
            logger.force();
            logger.close();
        } catch (Exception e) {
            assertTrue("Fallback should work without throwing exception", false);
        }
    }

    @Test
    public void testInitializeMethod() {
        // Test the initialize method with different property combinations
        outputStreamCaptor.reset();
        
        // Test with auto-enable disabled
        System.setProperty("iouring.enable_by_default", "false");
        IoUringBootstrap.initialize();
        
        String output1 = outputStreamCaptor.toString();
        assertTrue(output1.contains("=== IoUring Bootstrap Status ==="));
        outputStreamCaptor.reset();
        
        // Test with auto-enable enabled
        System.setProperty("iouring.enable_by_default", "true");
        System.setProperty("iouring.compression_enabled", "false");
        IoUringBootstrap.initialize();
        
        String output2 = outputStreamCaptor.toString();
        assertTrue(output2.contains("Global enabled: true"));
        outputStreamCaptor.reset();
        
        // Test with auto-enable and compression
        System.setProperty("iouring.compression_enabled", "true");
        IoUringBootstrap.initialize();
        
        String output3 = outputStreamCaptor.toString();
        assertTrue(output3.contains("compressed_iouring"));
    }

    @Test
    public void testGetAllOperationsWithIoUring() {
        // Test getAll operations work with IoUring configuration
        IoUringBootstrap.enableAll();
        
        Schema schema = new Schema(
            new String[]{"id", "data"}, 
            new DataType[]{DataType.INT, DataType.STRING}, 
            new int[]{0}, 
            null, 
            false
        );
        
        PrimaryIndex primaryIndex = StorageUtils.createPrimaryIndex(
            "test_getall", schema, true, false, false, 100);
        Table table = new Table("test_getall", schema, primaryIndex);
        Map<String, Table> catalog = new HashMap<>();
        catalog.put("test_getall", table);
        
        TransactionManager txManager = new TransactionManager(catalog, true);
        
        // Begin transaction
        txManager.beginTransaction(1L, 1, 0L, false);
        
        // Insert test data
        txManager.insert(table, new Object[]{1, "data1"});
        txManager.insert(table, new Object[]{2, "data2"});
        txManager.insert(table, new Object[]{3, "data3"});
        
        // Test getAll
        List<Object[]> allRecords = txManager.getAll(table);
        assertNotNull(allRecords);
        assertTrue(allRecords.size() >= 3);
        
        // Verify data integrity
        boolean found1 = false, found2 = false, found3 = false;
        for (Object[] record : allRecords) {
            int id = (Integer) record[0];
            if (id == 1) found1 = true;
            if (id == 2) found2 = true;
            if (id == 3) found3 = true;
        }
        
        assertTrue("All inserted records should be found", found1 && found2 && found3);
    }
} 