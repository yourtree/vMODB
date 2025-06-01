package dk.ku.di.dms.vms.modb.iouring;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dk.ku.di.dms.vms.modb.iouring.IoUringChannelGroup;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class IoUringChannelGroupTest extends TestBase {

    @Test
    @Timeout(10)
    void testGroupCreation() {
        ThreadFactory tf = Thread::new;
        IoUringChannelGroup group = IoUringChannelGroup.withFixedThreadPool(4, tf);
        
        assertNotNull(group);
        assertFalse(group.isShutdown());
        assertFalse(group.isTerminated());
        assertNotNull(group.ring());
        assertNotNull(group.executor());
        
        group.shutdown();
        assertTrue(group.isShutdown());
    }

    @Test
    @Timeout(10)
    void testGroupShutdown() throws InterruptedException {
        ThreadFactory tf = Thread::new;
        IoUringChannelGroup group = IoUringChannelGroup.withFixedThreadPool(2, tf);
        
        assertFalse(group.isShutdown());
        assertFalse(group.isTerminated());
        
        group.shutdown();
        assertTrue(group.isShutdown());
        
        boolean terminated = group.awaitTermination(5, TimeUnit.SECONDS);
        assertTrue(terminated);
        assertTrue(group.isTerminated());
    }

    @Test
    @Timeout(10)
    void testGroupShutdownNow() throws InterruptedException {
        ThreadFactory tf = Thread::new;
        IoUringChannelGroup group = IoUringChannelGroup.withFixedThreadPool(2, tf);
        
        group.shutdownNow();
        assertTrue(group.isShutdown());
        
        boolean terminated = group.awaitTermination(5, TimeUnit.SECONDS);
        assertTrue(terminated);
    }

    @Test
    @Timeout(10)
    void testThreadFactoryUsage() throws InterruptedException {
        AtomicReference<Thread> createdThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        
        ThreadFactory tf = (runnable) -> {
            Thread t = new Thread(runnable);
            createdThread.set(t);
            latch.countDown();
            return t;
        };
        
        IoUringChannelGroup group = IoUringChannelGroup.withFixedThreadPool(1, tf);
        
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertNotNull(createdThread.get());
        
        group.shutdown();
    }
} 