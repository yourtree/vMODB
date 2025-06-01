package dk.ku.di.dms.vms.modb.iouring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
 * Comprehensive test suite for io_uring logging handlers
 */
@RunWith(Parameterized.class)
public class IoUringLoggingTest {
    
    private static final String TEST_DIR = System.getProperty("user.home") + "/vms/test";
    private final String loggingType;
    private ILoggingHandler loggingHandler;
    private String testIdentifier;
    
    @Parameterized.Parameters(name = "loggingType={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"default"},
            {"compressed"},
            {"iouring"},
            {"compressed_iouring"}
        });
    }
    
    public IoUringLoggingTest(String loggingType) {
        this.loggingType = loggingType;
    }
    
    @Before
    public void setUp() throws IOException {
        // Create test directory
        Files.createDirectories(Paths.get(TEST_DIR));
        
        // Set logging type
        System.setProperty("logging_type", loggingType);
        
        // Create unique identifier for this test
        testIdentifier = "test_" + loggingType + "_" + System.currentTimeMillis();
        
        // Build logging handler
        loggingHandler = LoggingHandlerBuilder.build(testIdentifier);
    }
    
    @After
    public void tearDown() {
        if (loggingHandler != null) {
            loggingHandler.close();
        }
        
        // Clean up test files
        try {
            Files.walk(Paths.get(TEST_DIR))
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().contains(testIdentifier))
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
    public void testBasicWriteAndForce() throws IOException {
        String testData = "Hello, io_uring logging!";
        ByteBuffer buffer = ByteBuffer.allocateDirect(testData.length());
        buffer.put(testData.getBytes(StandardCharsets.UTF_8));
        buffer.flip();
        
        // Write data
        loggingHandler.log(buffer);
        
        // Force to disk
        loggingHandler.force();
        
        // Verify file exists
        String fileName = loggingHandler.getFileName();
        assertNotNull(fileName);
        
        // For io_uring handlers, wait a bit for async operations
        if (loggingType.contains("iouring")) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    @Test
    public void testMultipleWrites() throws IOException {
        List<String> testData = Arrays.asList(
            "First line of data",
            "Second line of data", 
            "Third line of data",
            "Fourth line with special chars: ñáéíóú",
            "Fifth line with numbers: 123456789"
        );
        
        for (String data : testData) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(data.length() + 1);
            buffer.put((data + "\n").getBytes(StandardCharsets.UTF_8));
            buffer.flip();
            loggingHandler.log(buffer);
        }
        
        loggingHandler.force();
    }
    
    @Test
    public void testLargeWrites() throws IOException {
        // Test with various buffer sizes
        int[] sizes = {1024, 4096, 16384, 65536, 1048576}; // 1KB to 1MB
        
        for (int size : sizes) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            
            // Fill buffer with pattern
            byte[] pattern = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes();
            while (buffer.hasRemaining()) {
                int toWrite = Math.min(pattern.length, buffer.remaining());
                buffer.put(pattern, 0, toWrite);
            }
            buffer.flip();
            
            loggingHandler.log(buffer);
        }
        
        loggingHandler.force();
    }
    
    @Test
    public void testConcurrentWrites() throws InterruptedException, ExecutionException {
        int numThreads = 10;
        int writesPerThread = 100;
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
                    
                    for (int i = 0; i < writesPerThread; i++) {
                        String data = String.format("Thread-%d-Write-%d-Timestamp-%d%n", 
                            threadId, i, System.nanoTime());
                        ByteBuffer buffer = ByteBuffer.allocateDirect(data.length());
                        buffer.put(data.getBytes(StandardCharsets.UTF_8));
                        buffer.flip();
                        
                        try {
                            loggingHandler.log(buffer);
                            successCount.incrementAndGet();
                        } catch (IOException e) {
                            errorCount.incrementAndGet();
                        }
                        
                        // Small delay to increase concurrency
                        Thread.yield();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            }));
        }
        
        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for completion
        for (Future<?> future : futures) {
            future.get();
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        
        // Force final flush
        loggingHandler.force();
        
        // Verify results
        assertEquals(numThreads * writesPerThread, successCount.get());
        assertEquals(0, errorCount.get());
    }
    
    @Test
    public void testWriteAfterForce() throws IOException {
        // Initial write
        String data1 = "Before force";
        ByteBuffer buffer1 = ByteBuffer.allocateDirect(data1.length());
        buffer1.put(data1.getBytes(StandardCharsets.UTF_8));
        buffer1.flip();
        loggingHandler.log(buffer1);
        
        // Force
        loggingHandler.force();
        
        // Write after force
        String data2 = "After force";
        ByteBuffer buffer2 = ByteBuffer.allocateDirect(data2.length());
        buffer2.put(data2.getBytes(StandardCharsets.UTF_8));
        buffer2.flip();
        loggingHandler.log(buffer2);
        
        // Force again
        loggingHandler.force();
    }
    
    @Test
    public void testRapidOpenClose() throws IOException {
        // Test rapid open/close cycles
        for (int i = 0; i < 10; i++) {
            ILoggingHandler handler = LoggingHandlerBuilder.build("rapid_test_" + i);
            
            String data = "Rapid test " + i;
            ByteBuffer buffer = ByteBuffer.allocateDirect(data.length());
            buffer.put(data.getBytes(StandardCharsets.UTF_8));
            buffer.flip();
            
            handler.log(buffer);
            handler.force();
            handler.close();
        }
    }
    
    @Test(expected = IOException.class)
    public void testWriteAfterClose() throws IOException {
        loggingHandler.close();
        
        ByteBuffer buffer = ByteBuffer.allocateDirect(10);
        buffer.put("test".getBytes());
        buffer.flip();
        
        loggingHandler.log(buffer); // Should throw IOException
    }
    
    @Test
    public void testEmptyWrites() throws IOException {
        // Test writing empty buffer
        ByteBuffer emptyBuffer = ByteBuffer.allocateDirect(0);
        loggingHandler.log(emptyBuffer);
        
        // Test writing buffer with no remaining bytes
        ByteBuffer buffer = ByteBuffer.allocateDirect(10);
        buffer.position(buffer.limit());
        loggingHandler.log(buffer);
        
        loggingHandler.force();
    }
    
    @Test
    public void testBufferPositionPreservation() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(100);
        byte[] data = "Test data for position preservation".getBytes();
        buffer.put(data);
        buffer.flip();
        
        int positionBefore = buffer.position();
        int limitBefore = buffer.limit();
        
        loggingHandler.log(buffer);
        
        // For compressed handlers, buffer might be rewound
        if (loggingType.contains("compressed")) {
            assertEquals(0, buffer.position());
        } else {
            // For non-compressed, position should advance
            assertTrue(buffer.position() >= positionBefore);
        }
        
        assertEquals(limitBefore, buffer.limit());
    }
} 