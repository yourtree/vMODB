package dk.ku.di.dms.vms.modb.iouring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IntKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.storage.record.IoUringRecordBufferContext;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;
import dk.ku.di.dms.vms.modb.utils.StorageUtils;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Comprehensive test suite for io_uring storage implementation
 */
@RunWith(Parameterized.class)
public class IoUringStorageTest {
    
    private static final String TEST_DIR = System.getProperty("user.home") + "/vms/test_storage";
    private final String storageBackend;
    private Schema schema;
    private String testTableName;
    
    @Parameterized.Parameters(name = "backend={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"default"},
            {"iouring"}
        });
    }
    
    public IoUringStorageTest(String storageBackend) {
        this.storageBackend = storageBackend;
    }
    
    @Before
    public void setUp() throws IOException {
        // Create test directory
        Files.createDirectories(Paths.get(TEST_DIR));
        
        // Set storage backend
        System.setProperty("storage_backend", storageBackend);
        System.setProperty("checkpointing", "true");
        
        // Create test schema
        schema = new Schema(
            new String[]{"id", "name", "age", "active"},
            new DataType[]{DataType.INT, DataType.STRING, DataType.INT, DataType.BOOL},
            new int[]{0}, // Primary key on id
            null,
            false
        );
        
        testTableName = "test_table_" + storageBackend + "_" + System.currentTimeMillis();
    }
    
    @After
    public void tearDown() {
        // Clean up test files
        try {
            Files.walk(Paths.get(TEST_DIR))
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().contains(testTableName))
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
        } catch (IOException e) {
            // Ignore
        }
    }
    
    @Test
    public void testRecordBufferContextCreation() throws IOException {
        int maxRecords = 1000;
        int recordSize = schema.getRecordSizeWithHeader();
        
        RecordBufferContext bufferContext = StorageUtils.loadRecordBuffer(
            maxRecords, recordSize, testTableName, true);
        
        assertNotNull(bufferContext);
        
        if ("iouring".equals(storageBackend)) {
            assertTrue(bufferContext instanceof IoUringRecordBufferContext);
        } else {
            assertFalse(bufferContext instanceof IoUringRecordBufferContext);
        }
        
        // Test force operation
        bufferContext.force();
        
        if (bufferContext instanceof IoUringRecordBufferContext) {
            ((IoUringRecordBufferContext) bufferContext).close();
        }
    }
    
    @Test
    public void testPrimaryIndexOperations() {
        int maxRecords = 10000;
        PrimaryIndex primaryIndex = StorageUtils.createPrimaryIndex(
            testTableName, schema, true, true, false, maxRecords);
        
        assertNotNull(primaryIndex);
        
        // Test data
        List<Object[]> testRecords = Arrays.asList(
            new Object[]{1, "Alice", 25, true},
            new Object[]{2, "Bob", 30, false},
            new Object[]{3, "Charlie", 35, true},
            new Object[]{4, "David", 40, false},
            new Object[]{5, "Eve", 45, true}
        );
        
        // Insert records
        for (Object[] record : testRecords) {
            primaryIndex.insert(null, IntKey.of((Integer) record[0]), record);
        }
        
        // Lookup records
        for (Object[] expected : testRecords) {
            Object[] actual = primaryIndex.lookupByKey(null, IntKey.of((Integer) expected[0]));
            assertNotNull(actual);
            assertEquals(expected[0], actual[0]);
            assertEquals(expected[1], actual[1]);
            assertEquals(expected[2], actual[2]);
            assertEquals(expected[3], actual[3]);
        }
        
        // Update record
        Object[] updatedRecord = new Object[]{2, "Bob Updated", 31, true};
        primaryIndex.update(null, IntKey.of(2), updatedRecord);
        
        Object[] retrieved = primaryIndex.lookupByKey(null, IntKey.of(2));
        assertEquals("Bob Updated", retrieved[1]);
        assertEquals(31, retrieved[2]);
        assertEquals(true, retrieved[3]);
        
        // Delete record
        primaryIndex.remove(null, IntKey.of(3));
        assertFalse(primaryIndex.exists(null, IntKey.of(3)));
        
        // Test checkpoint
        primaryIndex.checkpoint(Long.MAX_VALUE);
    }
    
    @Test
    public void testConcurrentOperations() throws InterruptedException, ExecutionException {
        int maxRecords = 100000;
        PrimaryIndex primaryIndex = StorageUtils.createPrimaryIndex(
            testTableName, schema, true, true, false, maxRecords);
        
        int numThreads = 10;
        int operationsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        List<Future<?>> futures = new ArrayList<>();
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    Random random = new Random(threadId);
                    
                    for (int i = 0; i < operationsPerThread; i++) {
                        int id = threadId * operationsPerThread + i;
                        
                        try {
                            // Insert
                            Object[] record = new Object[]{
                                id,
                                "User-" + id,
                                20 + random.nextInt(50),
                                random.nextBoolean()
                            };
                            primaryIndex.insert(null, IntKey.of(id), record);
                            
                            // Read
                            Object[] retrieved = primaryIndex.lookupByKey(null, IntKey.of(id));
                            assertNotNull(retrieved);
                            
                            // Update occasionally
                            if (random.nextDouble() < 0.3) {
                                record[2] = (Integer) record[2] + 1;
                                primaryIndex.update(null, IntKey.of(id), record);
                            }
                            
                            // Delete occasionally
                            if (random.nextDouble() < 0.1) {
                                primaryIndex.remove(null, IntKey.of(id));
                            }
                            
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            }));
        }
        
        // Start all threads
        startLatch.countDown();
        
        // Wait for completion
        for (Future<?> future : futures) {
            future.get();
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        
        // Checkpoint
        primaryIndex.checkpoint(Long.MAX_VALUE);
        
        // Verify
        assertTrue(successCount.get() > 0);
        assertEquals(0, errorCount.get());
    }
    
    @Test
    public void testForceOperationPerformance() throws IOException {
        int maxRecords = 10000;
        RecordBufferContext bufferContext = StorageUtils.loadRecordBuffer(
            maxRecords, schema.getRecordSizeWithHeader(), testTableName, true);
        
        UniqueHashBufferIndex index = new UniqueHashBufferIndex(
            bufferContext, schema, schema.getPrimaryKeyColumns(), maxRecords);
        
        // Insert some records
        for (int i = 0; i < 1000; i++) {
            Object[] record = new Object[]{i, "Test-" + i, 20 + i % 50, i % 2 == 0};
            index.insert(IntKey.of(i), record);
        }
        
        // Measure force operation time
        long startTime = System.currentTimeMillis();
        int forceCount = 10;
        
        for (int i = 0; i < forceCount; i++) {
            bufferContext.force();
        }
        
        long endTime = System.currentTimeMillis();
        long avgTime = (endTime - startTime) / forceCount;
        
        System.out.println("Storage backend: " + storageBackend);
        System.out.println("Average force() time: " + avgTime + " ms");
        
        if (bufferContext instanceof IoUringRecordBufferContext) {
            ((IoUringRecordBufferContext) bufferContext).close();
        }
    }
    
    @Test
    public void testAsyncWriteRegions() throws IOException {
        if (!"iouring".equals(storageBackend)) {
            // Skip test for non-io_uring backend
            return;
        }
        
        int maxRecords = 1000;
        MemorySegment segment = StorageUtils.mapFileIntoMemorySegment(
            maxRecords * schema.getRecordSizeWithHeader(), testTableName, true);
        
        IoUringRecordBufferContext ioUringContext = IoUringRecordBufferContext.build(
            segment, StorageUtils.getBasePath() + testTableName + ".data",
            maxRecords * schema.getRecordSizeWithHeader());
        
        // Test async write regions
        byte[] testData = "Test async write region".getBytes();
        ByteBuffer buffer = ByteBuffer.allocateDirect(testData.length);
        buffer.put(testData);
        buffer.flip();
        
        // Write to different regions
        for (int i = 0; i < 10; i++) {
            long offset = i * 1024;
            buffer.rewind();
            ioUringContext.writeRegionAsync(offset, buffer);
        }
        
        // Force to ensure writes complete
        ioUringContext.force();
        
        // Clean up
        ioUringContext.close();
    }
    
    @Test
    public void testRecoveryAfterCrash() throws IOException {
        int maxRecords = 1000;
        String fileName = testTableName + "_recovery";
        
        // First, create and populate an index
        {
            RecordBufferContext bufferContext = StorageUtils.loadRecordBuffer(
                maxRecords, schema.getRecordSizeWithHeader(), fileName, true);
            
            UniqueHashBufferIndex index = new UniqueHashBufferIndex(
                bufferContext, schema, schema.getPrimaryKeyColumns(), maxRecords);
            
            // Insert test data
            for (int i = 0; i < 100; i++) {
                Object[] record = new Object[]{i, "Recovery-" + i, 25, true};
                index.insert(IntKey.of(i), record);
            }
            
            // Force to disk
            bufferContext.force();
            
            if (bufferContext instanceof IoUringRecordBufferContext) {
                ((IoUringRecordBufferContext) bufferContext).close();
            }
        }
        
        // Simulate restart - open the same file
        {
            RecordBufferContext bufferContext = StorageUtils.loadRecordBuffer(
                maxRecords, schema.getRecordSizeWithHeader(), fileName, false);
            
            UniqueHashBufferIndex index = new UniqueHashBufferIndex(
                bufferContext, schema, schema.getPrimaryKeyColumns(), maxRecords);
            
            // Verify data is still there
            for (int i = 0; i < 100; i++) {
                Object[] record = index.lookupByKey(IntKey.of(i));
                assertNotNull("Record " + i + " should exist", record);
                assertEquals(i, record[0]);
                assertEquals("Recovery-" + i, record[1]);
            }
            
            if (bufferContext instanceof IoUringRecordBufferContext) {
                ((IoUringRecordBufferContext) bufferContext).close();
            }
        }
    }
} 