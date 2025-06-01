package dk.ku.di.dms.vms.modb.iouring;

import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IntKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.utils.StorageUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 * Performance benchmark comparing traditional I/O vs io_uring
 */
public class IoUringPerformanceBenchmark {
    
    private static final int WARMUP_ITERATIONS = 1000;
    private static final int TEST_ITERATIONS = 10000;
    private static final int CONCURRENT_THREADS = 10;
    
    @Test
    public void benchmarkLoggingHandlers() throws IOException {
        String[] loggingTypes = {"default", "compressed", "iouring", "compressed_iouring"};
        Map<String, BenchmarkResult> results = new HashMap<>();
        
        for (String loggingType : loggingTypes) {
            System.out.println("\n=== Benchmarking " + loggingType + " ===");
            results.put(loggingType, benchmarkLoggingHandler(loggingType));
        }
        
        // Print comparison
        System.out.println("\n=== Performance Comparison ===");
        System.out.println("Type\t\t\tAvg Write (µs)\tThroughput (MB/s)\tAvg Force (ms)");
        System.out.println("----\t\t\t--------------\t-----------------\t--------------");
        
        for (Map.Entry<String, BenchmarkResult> entry : results.entrySet()) {
            BenchmarkResult result = entry.getValue();
            System.out.printf("%-20s\t%.2f\t\t%.2f\t\t\t%.2f%n",
                entry.getKey(),
                result.avgWriteTimeUs,
                result.throughputMBps,
                result.avgForceTimeMs);
        }
    }
    
    private BenchmarkResult benchmarkLoggingHandler(String loggingType) throws IOException {
        System.setProperty("logging_type", loggingType);
        ILoggingHandler handler = LoggingHandlerBuilder.build("benchmark_" + loggingType);
        
        try {
            // Prepare test data
            String testData = generateTestData(1024); // 1KB per write
            ByteBuffer buffer = ByteBuffer.allocateDirect(testData.length());
            
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                buffer.clear();
                buffer.put(testData.getBytes(StandardCharsets.UTF_8));
                buffer.flip();
                handler.log(buffer);
            }
            handler.force();
            
            // Benchmark writes
            long totalWriteTime = 0;
            long totalBytes = 0;
            
            for (int i = 0; i < TEST_ITERATIONS; i++) {
                buffer.clear();
                buffer.put(testData.getBytes(StandardCharsets.UTF_8));
                buffer.flip();
                
                long start = System.nanoTime();
                handler.log(buffer);
                long end = System.nanoTime();
                
                totalWriteTime += (end - start);
                totalBytes += testData.length();
            }
            
            // Benchmark force operations
            long totalForceTime = 0;
            int forceIterations = 100;
            
            for (int i = 0; i < forceIterations; i++) {
                long start = System.nanoTime();
                handler.force();
                long end = System.nanoTime();
                totalForceTime += (end - start);
            }
            
            // Calculate results
            BenchmarkResult result = new BenchmarkResult();
            result.avgWriteTimeUs = (totalWriteTime / TEST_ITERATIONS) / 1000.0;
            result.throughputMBps = (totalBytes / 1024.0 / 1024.0) / (totalWriteTime / 1_000_000_000.0);
            result.avgForceTimeMs = (totalForceTime / forceIterations) / 1_000_000.0;
            
            return result;
        } finally {
            handler.close();
        }
    }
    
    @Test
    public void benchmarkStorageBackends() throws IOException {
        String[] backends = {"default", "iouring"};
        Map<String, StorageBenchmarkResult> results = new HashMap<>();
        
        Schema schema = new Schema(
            new String[]{"id", "data", "timestamp"},
            new DataType[]{DataType.INT, DataType.STRING, DataType.LONG},
            new int[]{0},
            null,
            false
        );
        
        for (String backend : backends) {
            System.out.println("\n=== Benchmarking storage backend: " + backend + " ===");
            results.put(backend, benchmarkStorageBackend(backend, schema));
        }
        
        // Print comparison
        System.out.println("\n=== Storage Performance Comparison ===");
        System.out.println("Backend\t\tInsert (µs)\tLookup (µs)\tUpdate (µs)\tForce (ms)");
        System.out.println("-------\t\t-----------\t-----------\t-----------\t----------");
        
        for (Map.Entry<String, StorageBenchmarkResult> entry : results.entrySet()) {
            StorageBenchmarkResult result = entry.getValue();
            System.out.printf("%-15s\t%.2f\t\t%.2f\t\t%.2f\t\t%.2f%n",
                entry.getKey(),
                result.avgInsertTimeUs,
                result.avgLookupTimeUs,
                result.avgUpdateTimeUs,
                result.avgForceTimeMs);
        }
    }
    
    private StorageBenchmarkResult benchmarkStorageBackend(String backend, Schema schema) throws IOException {
        System.setProperty("storage_backend", backend);
        String tableName = "benchmark_" + backend + "_" + System.currentTimeMillis();
        
        int maxRecords = 100000;
        RecordBufferContext bufferContext = StorageUtils.loadRecordBuffer(
            maxRecords, schema.getRecordSizeWithHeader(), tableName, true);
        
        UniqueHashBufferIndex index = new UniqueHashBufferIndex(
            bufferContext, schema, schema.getPrimaryKeyColumns(), maxRecords);
        
        try {
            StorageBenchmarkResult result = new StorageBenchmarkResult();
            Random random = new Random();
            
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                Object[] record = new Object[]{i, "Warmup-" + i, System.currentTimeMillis()};
                index.insert(IntKey.of(i), record);
            }
            
            // Benchmark inserts
            long totalInsertTime = 0;
            int insertStart = WARMUP_ITERATIONS;
            
            for (int i = insertStart; i < insertStart + TEST_ITERATIONS; i++) {
                Object[] record = new Object[]{i, "Test-" + i, System.currentTimeMillis()};
                
                long start = System.nanoTime();
                index.insert(IntKey.of(i), record);
                long end = System.nanoTime();
                
                totalInsertTime += (end - start);
            }
            
            result.avgInsertTimeUs = (totalInsertTime / TEST_ITERATIONS) / 1000.0;
            
            // Benchmark lookups
            long totalLookupTime = 0;
            
            for (int i = 0; i < TEST_ITERATIONS; i++) {
                int key = random.nextInt(insertStart + TEST_ITERATIONS);
                
                long start = System.nanoTime();
                index.lookupByKey(IntKey.of(key));
                long end = System.nanoTime();
                
                totalLookupTime += (end - start);
            }
            
            result.avgLookupTimeUs = (totalLookupTime / TEST_ITERATIONS) / 1000.0;
            
            // Benchmark updates
            long totalUpdateTime = 0;
            
            for (int i = 0; i < TEST_ITERATIONS; i++) {
                int key = random.nextInt(WARMUP_ITERATIONS);
                Object[] record = new Object[]{key, "Updated-" + i, System.currentTimeMillis()};
                
                long start = System.nanoTime();
                index.update(IntKey.of(key), record);
                long end = System.nanoTime();
                
                totalUpdateTime += (end - start);
            }
            
            result.avgUpdateTimeUs = (totalUpdateTime / TEST_ITERATIONS) / 1000.0;
            
            // Benchmark force operations
            long totalForceTime = 0;
            int forceIterations = 10;
            
            for (int i = 0; i < forceIterations; i++) {
                long start = System.nanoTime();
                bufferContext.force();
                long end = System.nanoTime();
                totalForceTime += (end - start);
            }
            
            result.avgForceTimeMs = (totalForceTime / forceIterations) / 1_000_000.0;
            
            return result;
        } finally {
            if (bufferContext instanceof dk.ku.di.dms.vms.modb.storage.record.IoUringRecordBufferContext) {
                ((dk.ku.di.dms.vms.modb.storage.record.IoUringRecordBufferContext) bufferContext).close();
            }
        }
    }
    
    @Test
    public void benchmarkConcurrentAccess() throws InterruptedException, ExecutionException {
        String[] backends = {"default", "iouring"};
        
        for (String backend : backends) {
            System.out.println("\n=== Concurrent Access Benchmark: " + backend + " ===");
            benchmarkConcurrentStorageAccess(backend);
        }
    }
    
    private void benchmarkConcurrentStorageAccess(String backend) throws InterruptedException, ExecutionException {
        System.setProperty("storage_backend", backend);
        System.setProperty("logging_type", backend.equals("iouring") ? "iouring" : "default");
        
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicLong totalOperations = new AtomicLong(0);
        AtomicLong totalTime = new AtomicLong(0);
        
        List<Future<ThreadResult>> futures = new ArrayList<>();
        
        for (int t = 0; t < CONCURRENT_THREADS; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                ILoggingHandler handler = LoggingHandlerBuilder.build("concurrent_" + backend + "_" + threadId);
                
                try {
                    startLatch.await();
                    
                    long threadOperations = 0;
                    long threadStartTime = System.nanoTime();
                    
                    for (int i = 0; i < TEST_ITERATIONS / CONCURRENT_THREADS; i++) {
                        String data = "Thread-" + threadId + "-Operation-" + i + "-" + System.nanoTime();
                        ByteBuffer buffer = ByteBuffer.allocateDirect(data.length());
                        buffer.put(data.getBytes(StandardCharsets.UTF_8));
                        buffer.flip();
                        
                        handler.log(buffer);
                        threadOperations++;
                        
                        if (i % 100 == 0) {
                            handler.force();
                        }
                    }
                    
                    handler.force();
                    long threadEndTime = System.nanoTime();
                    
                    ThreadResult result = new ThreadResult();
                    result.operations = threadOperations;
                    result.timeNanos = threadEndTime - threadStartTime;
                    
                    return result;
                } finally {
                    handler.close();
                }
            }));
        }
        
        // Start all threads
        long startTime = System.nanoTime();
        startLatch.countDown();
        
        // Collect results
        for (Future<ThreadResult> future : futures) {
            ThreadResult result = future.get();
            totalOperations.addAndGet(result.operations);
            totalTime.addAndGet(result.timeNanos);
        }
        
        long endTime = System.nanoTime();
        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        
        // Calculate and print results
        double totalTimeSec = (endTime - startTime) / 1_000_000_000.0;
        double avgTimePerThreadSec = (totalTime.get() / CONCURRENT_THREADS) / 1_000_000_000.0;
        double throughput = totalOperations.get() / totalTimeSec;
        
        System.out.printf("Total operations: %d%n", totalOperations.get());
        System.out.printf("Total time: %.2f seconds%n", totalTimeSec);
        System.out.printf("Average time per thread: %.2f seconds%n", avgTimePerThreadSec);
        System.out.printf("Throughput: %.2f operations/second%n", throughput);
    }
    
    private String generateTestData(int size) {
        StringBuilder sb = new StringBuilder(size);
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        
        for (int i = 0; i < size; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        
        return sb.toString();
    }
    
    private static class BenchmarkResult {
        double avgWriteTimeUs;
        double throughputMBps;
        double avgForceTimeMs;
    }
    
    private static class StorageBenchmarkResult {
        double avgInsertTimeUs;
        double avgLookupTimeUs;
        double avgUpdateTimeUs;
        double avgForceTimeMs;
    }
    
    private static class ThreadResult {
        long operations;
        long timeNanos;
    }
} 