package dk.ku.di.dms.vms.modb.iouring;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Edge case tests for io_uring implementation
 */
public class IoUringEdgeCaseTest {
    
    @Test
    public void testVeryLargeWrite() throws IOException {
        System.setProperty("logging_type", "iouring");
        ILoggingHandler handler = LoggingHandlerBuilder.build("edge_case_large");
        
        try {
            // Test 100MB write
            int size = 100 * 1024 * 1024;
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            
            // Fill with pattern
            byte[] pattern = new byte[1024];
            new Random().nextBytes(pattern);
            
            while (buffer.hasRemaining()) {
                int toWrite = Math.min(pattern.length, buffer.remaining());
                buffer.put(pattern, 0, toWrite);
            }
            buffer.flip();
            
            long start = System.currentTimeMillis();
            handler.log(buffer);
            handler.force();
            long end = System.currentTimeMillis();
            
            System.out.println("100MB write took: " + (end - start) + " ms");
            
        } finally {
            handler.close();
        }
    }
    
    @Test
    public void testRapidSmallWrites() throws IOException {
        System.setProperty("logging_type", "iouring");
        ILoggingHandler handler = LoggingHandlerBuilder.build("edge_case_rapid");
        
        try {
            // Test 100,000 small writes
            int numWrites = 100000;
            byte[] data = "Small write test\n".getBytes(StandardCharsets.UTF_8);
            
            long start = System.currentTimeMillis();
            
            for (int i = 0; i < numWrites; i++) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
                buffer.put(data);
                buffer.flip();
                handler.log(buffer);
                
                if (i % 10000 == 0) {
                    handler.force();
                }
            }
            
            handler.force();
            long end = System.currentTimeMillis();
            
            double throughput = (numWrites * data.length / 1024.0 / 1024.0) / ((end - start) / 1000.0);
            System.out.println("Small writes throughput: " + throughput + " MB/s");
            
        } finally {
            handler.close();
        }
    }
    
    @Test
    public void testMemoryPressure() throws IOException, InterruptedException {
        System.setProperty("logging_type", "iouring");
        
        // Create many handlers simultaneously
        List<ILoggingHandler> handlers = new ArrayList<>();
        
        try {
            for (int i = 0; i < 50; i++) {
                handlers.add(LoggingHandlerBuilder.build("memory_pressure_" + i));
            }
            
            // Write to all handlers
            String data = "Memory pressure test data\n";
            ByteBuffer buffer = ByteBuffer.allocateDirect(data.length());
            
            for (int round = 0; round < 100; round++) {
                for (ILoggingHandler handler : handlers) {
                    buffer.clear();
                    buffer.put(data.getBytes(StandardCharsets.UTF_8));
                    buffer.flip();
                    handler.log(buffer);
                }
                
                if (round % 10 == 0) {
                    for (ILoggingHandler handler : handlers) {
                        handler.force();
                    }
                }
            }
            
            System.out.println("Memory pressure test completed with " + handlers.size() + " handlers");
            
        } finally {
            for (ILoggingHandler handler : handlers) {
                handler.close();
            }
        }
    }
    
    @Test
    public void testInterruptedThread() throws InterruptedException, ExecutionException {
        System.setProperty("logging_type", "iouring");
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        
        Future<Boolean> future = executor.submit(() -> {
            ILoggingHandler handler = LoggingHandlerBuilder.build("interrupted_test");
            
            try {
                // Simulate long-running write operation
                for (int i = 0; i < 1000; i++) {
                    if (Thread.interrupted()) {
                        System.out.println("Thread interrupted at iteration " + i);
                        return false;
                    }
                    
                    String data = "Iteration " + i + "\n";
                    ByteBuffer buffer = ByteBuffer.allocateDirect(data.length());
                    buffer.put(data.getBytes(StandardCharsets.UTF_8));
                    buffer.flip();
                    
                    handler.log(buffer);
                    Thread.sleep(10); // Simulate slow operation
                }
                
                handler.force();
                return true;
                
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted during sleep");
                Thread.currentThread().interrupt();
                return false;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            } finally {
                handler.close();
            }
        });
        
        // Let it run for a bit
        Thread.sleep(500);
        
        // Interrupt the thread
        future.cancel(true);
        
        // Verify it was interrupted
        try {
            Boolean result = future.get();
            assertFalse("Operation should have been interrupted", result);
        } catch (CancellationException e) {
            // Expected
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
    
    @Test
    public void testSystemResourceExhaustion() {
        // Test behavior when system resources are exhausted
        List<ILoggingHandler> handlers = new ArrayList<>();
        
        try {
            // Try to create many handlers until failure
            for (int i = 0; i < 1000; i++) {
                try {
                    System.setProperty("logging_type", i % 2 == 0 ? "iouring" : "default");
                    handlers.add(LoggingHandlerBuilder.build("exhaustion_test_" + i));
                } catch (Exception e) {
                    System.out.println("Resource exhaustion at handler " + i + ": " + e.getMessage());
                    break;
                }
            }
            
            System.out.println("Successfully created " + handlers.size() + " handlers");
            
            // Try to write to all handlers
            String data = "Test data\n";
            int successCount = 0;
            
            for (ILoggingHandler handler : handlers) {
                try {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(data.length());
                    buffer.put(data.getBytes(StandardCharsets.UTF_8));
                    buffer.flip();
                    handler.log(buffer);
                    successCount++;
                } catch (Exception e) {
                    // Expected for some handlers
                }
            }
            
            System.out.println("Successfully wrote to " + successCount + " handlers");
            
        } finally {
            // Clean up
            for (ILoggingHandler handler : handlers) {
                try {
                    handler.close();
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        }
    }
} 