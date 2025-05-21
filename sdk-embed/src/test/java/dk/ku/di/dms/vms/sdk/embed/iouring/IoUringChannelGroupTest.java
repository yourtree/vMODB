package dk.ku.di.dms.vms.sdk.embed.iouring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.channels.CompletionHandler;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class IoUringChannelGroupTest {
    private IoUringChannelGroup channelGroup;
    private static final int RING_SIZE = 1024;
    private static final int THREAD_POOL_SIZE = 2;

    @BeforeEach
    public void setUp() {
        channelGroup = IoUringChannelGroup.create(RING_SIZE, THREAD_POOL_SIZE);
        assertNotNull(channelGroup, "Channel group should be created");
    }

    @AfterEach
    public void tearDown() {
        if (channelGroup != null) {
            channelGroup.shutdown();
        }
    }

    @Test
    public void testConcurrentOperations() throws Exception {
        final int NUM_CHANNELS = 10;
        final CountDownLatch latch = new CountDownLatch(NUM_CHANNELS);
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger failureCount = new AtomicInteger(0);

        // Create multiple channels and perform operations concurrently
        for (int i = 0; i < NUM_CHANNELS; i++) {
            final IoUringChannel channel = IoUringChannel.create(channelGroup);
            assertNotNull(channel, "Channel should be created");

            // Configure socket
            channel.configureSocket(8192, 8192);
            channel.configureBlocking(false);

            // Perform some operations
            channel.connect("localhost", 12345, new CompletionHandler<Void>() {
                @Override
                public void completed(Void result, Object attachment) {
                    successCount.incrementAndGet();
                    latch.countDown();
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    failureCount.incrementAndGet();
                    latch.countDown();
                }
            });
        }

        // Wait for all operations to complete
        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertTrue(completed, "All operations should complete within timeout");
        
        // Verify results
        int total = successCount.get() + failureCount.get();
        assertEquals(NUM_CHANNELS, total, "All operations should complete");
    }

    @Test
    public void testShutdown() throws Exception {
        // Create a channel
        IoUringChannel channel = IoUringChannel.create(channelGroup);
        assertNotNull(channel, "Channel should be created");

        // Shutdown the group
        channelGroup.shutdown();

        // Verify that new operations fail
        assertThrows(IllegalStateException.class, () -> {
            channel.connect("localhost", 12345, new CompletionHandler<Void>() {
                @Override
                public void completed(Void result, Object attachment) {}

                @Override
                public void failed(Throwable exc, Object attachment) {}
            });
        }, "Operation should fail after shutdown");
    }

    @Test
    public void testResourceCleanup() throws Exception {
        // Create multiple channels
        final int NUM_CHANNELS = 5;
        IoUringChannel[] channels = new IoUringChannel[NUM_CHANNELS];
        
        for (int i = 0; i < NUM_CHANNELS; i++) {
            channels[i] = IoUringChannel.create(channelGroup);
            assertNotNull(channels[i], "Channel should be created");
        }

        // Close all channels
        for (IoUringChannel channel : channels) {
            channel.close();
            assertFalse(channel.isOpen(), "Channel should be closed");
        }

        // Shutdown group
        channelGroup.shutdown();
    }
} 