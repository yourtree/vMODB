package dk.ku.di.dms.vms.modb.iouring;

import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.spi.AsynchronousChannelProvider;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * AsynchronousChannelGroup implementation using io_uring.
 */
public final class IoUringChannelGroup extends AsynchronousChannelGroup {

    private static final class StubProvider extends AsynchronousChannelProvider {
        static final StubProvider INSTANCE = new StubProvider();

        @Override public AsynchronousChannelGroup openAsynchronousChannelGroup(
                int nThreads, ThreadFactory tf) { throw new UnsupportedOperationException(); }

        @Override public AsynchronousChannelGroup openAsynchronousChannelGroup(
                ExecutorService es, int initialSize) { throw new UnsupportedOperationException(); }

        @Override public AsynchronousServerSocketChannel openAsynchronousServerSocketChannel(
                AsynchronousChannelGroup g) { throw new UnsupportedOperationException(); }

        @Override public AsynchronousSocketChannel openAsynchronousSocketChannel(
                AsynchronousChannelGroup g) { throw new UnsupportedOperationException(); }
    }

    private final IoUring ring;
    private final Thread eventThread;
    private final ExecutorService executorService;
    private volatile boolean shutdown = false;

    private IoUringChannelGroup(ThreadFactory threadFactory) {
        super(StubProvider.INSTANCE);         
        this.ring = new IoUring();
        this.executorService = Executors.newCachedThreadPool(threadFactory);
        
        this.eventThread = threadFactory.newThread(() -> {
            try {
                ring.loop();
            } catch (Exception e) {
                if (!shutdown) {
                    // Log error if needed
                }
            }
        });
        this.eventThread.setDaemon(true);
        this.eventThread.start();
    }

    /**
     * Creates a new IoUringChannelGroup with a fixed thread pool.
     * Note: nThreads is ignored in this implementation - only one event loop thread is used.
     * 
     * @param _nThreads the number of threads (ignored)
     * @param threadFactory the thread factory
     * @return new channel group
     */
    public static IoUringChannelGroup withFixedThreadPool(int _nThreads, ThreadFactory threadFactory) {
        return new IoUringChannelGroup(threadFactory);
    }

    /**
     * Creates a new IoUringChannelGroup with a thread pool using the provided ExecutorService.
     * Note: The ExecutorService is ignored for event loop thread creation - only one event loop thread is used.
     * A default thread factory is used for the event loop thread.
     * 
     * @param executorService the executor service (used for callbacks but not for event loop)
     * @return new channel group
     */
    public static IoUringChannelGroup withThreadPool(ExecutorService executorService) {
        return new IoUringChannelGroup(Thread::new);
    }

    /**
     * Gets the underlying IoUring instance.
     * Package-private for use by channel implementations.
     */
    IoUring ring() {
        return ring;
    }

    /**
     * Gets the executor service for callback execution.
     */
    ExecutorService executor() {
        return executorService;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && !eventThread.isAlive();
    }

    @Override
    public void shutdown() {
        if (!shutdown) {
            shutdown = true;
            ring.close();
            // Interrupt the event thread to break it out of any blocking io_uring calls
            eventThread.interrupt();
            executorService.shutdown();
        }
    }

    @Override
    public void shutdownNow() {
        shutdown();
        eventThread.interrupt();
        executorService.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        shutdown();
        
        long timeoutMillis = unit.toMillis(timeout);
        long startTime = System.currentTimeMillis();
        
        // First wait for the executor service
        boolean executorTerminated = executorService.awaitTermination(timeout, unit);
        if (!executorTerminated) {
            return false;
        }
        
        // Calculate remaining time
        long elapsed = System.currentTimeMillis() - startTime;
        long remaining = timeoutMillis - elapsed;
        
        if (remaining <= 0) {
            return isTerminated();
        }
        
        // Wait for the event thread to finish
        eventThread.join(remaining);
        
        return isTerminated();
    }
} 