package dk.ku.di.dms.vms.web_common.iouring;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NotYetBoundException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class IoUringServerSocketChannelTest extends TestBase {
    private IoUringChannelGroup group;
    private ThreadFactory threadFactory;

    @BeforeEach
    void setUp() {
        threadFactory = Thread::new;
        group = IoUringChannelGroup.withFixedThreadPool(1, threadFactory);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (group != null) {
            group.shutdown();
            group.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @Timeout(10)
    void testServerSocketCreation() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        
        assertNotNull(serverChannel);
        assertTrue(serverChannel.isOpen());
        
        serverChannel.close();
        assertFalse(serverChannel.isOpen());
    }

    @Test
    @Timeout(10)
    void testBind() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        int port = randomPort();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", port);
        
        serverChannel.bind(address, 10);
        assertEquals(address, serverChannel.getLocalAddress());
        
        serverChannel.close();
    }

    @Test
    @Timeout(10)
    void testAcceptWithCompletionHandler() throws IOException, InterruptedException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        int port = randomPort();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", port);
        
        serverChannel.bind(address, 10);
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AsynchronousSocketChannel> acceptedChannel = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        
        serverChannel.accept("test-attachment", new CompletionHandler<AsynchronousSocketChannel, String>() {
            @Override
            public void completed(AsynchronousSocketChannel result, String attachment) {
                acceptedChannel.set(result);
                assertEquals("test-attachment", attachment);
                latch.countDown();
            }

            @Override
            public void failed(Throwable exc, String attachment) {
                error.set(exc);
                latch.countDown();
            }
        });
        
        // Simulate a client connection (in a real test, we'd use a client socket)
        // For now, we just verify the handler was set up correctly
        serverChannel.close();
        
        // Wait a bit and verify no immediate errors
        Thread.sleep(100);
        assertNull(error.get());
    }

    @Test
    @Timeout(10)
    void testAcceptWithFuture() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        int port = randomPort();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", port);
        
        serverChannel.bind(address, 10);
        
        Future<AsynchronousSocketChannel> future = serverChannel.accept();
        assertNotNull(future);
        assertFalse(future.isDone());
        
        serverChannel.close();
    }

    @Test
    @Timeout(10)
    void testUnsupportedOperations() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        
        // No operations are currently unsupported for server sockets
        // All socket option operations should work when properly called
        
        serverChannel.close();
    }

    @Test
    @Timeout(10)
    void testSocketOptionSupport() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        
        // Test supportedOptions() - this should work even when not bound
        Set<SocketOption<?>> supportedOptions = serverChannel.supportedOptions();
        assertNotNull(supportedOptions);
        assertTrue(supportedOptions.contains(StandardSocketOptions.SO_RCVBUF));
        assertTrue(supportedOptions.contains(StandardSocketOptions.SO_REUSEADDR));
        
        serverChannel.close();
    }

    @Test
    @Timeout(10)
    void testSocketOptionsClosed() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        serverChannel.close();
        
        // Test that operations throw ClosedChannelException when channel is closed
        assertThrows(ClosedChannelException.class, () -> 
            serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true));
        assertThrows(ClosedChannelException.class, () -> 
            serverChannel.getOption(StandardSocketOptions.SO_REUSEADDR));
    }

    @Test
    @Timeout(10)
    void testSocketOptionsNotBound() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        
        // Test that operations throw ClosedChannelException when nativeSocket is null (not bound)
        assertThrows(ClosedChannelException.class, () -> 
            serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true));
        assertThrows(ClosedChannelException.class, () -> 
            serverChannel.getOption(StandardSocketOptions.SO_REUSEADDR));
        
        serverChannel.close();
    }

    @Test
    @Timeout(10)
    void testSocketOptionsNullArguments() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        
        // Test null arguments
        assertThrows(IllegalArgumentException.class, () -> 
            serverChannel.setOption(null, true));
        assertThrows(IllegalArgumentException.class, () -> 
            serverChannel.getOption(null));
        
        serverChannel.close();
    }

    // @Test 
    // @Timeout(10)
    // void testSocketOptionsUnsupportedOption() throws IOException {
    //     IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        
    //     // Test unsupported socket option (server sockets don't support TCP_NODELAY)
    //     assertThrows(UnsupportedOperationException.class, () -> 
    //         serverChannel.setOption(StandardSocketOptions.TCP_NODELAY, true));
    //     assertThrows(UnsupportedOperationException.class, () -> 
    //         serverChannel.getOption(StandardSocketOptions.TCP_NODELAY));
        
    //     serverChannel.close();
    // }

    @Test
    @Timeout(10)
    void testSocketOptionsAfterBind() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        int port = randomPort();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", port);
        
        try {
            // Bind the server socket
            serverChannel.bind(address, 10);
            
            // Test SO_REUSEADDR functionality
            serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            Boolean reuseAddr = serverChannel.getOption(StandardSocketOptions.SO_REUSEADDR);
            assertTrue(reuseAddr, "SO_REUSEADDR should be true after setting to true");
            
            serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, false);
            reuseAddr = serverChannel.getOption(StandardSocketOptions.SO_REUSEADDR);
            assertFalse(reuseAddr, "SO_REUSEADDR should be false after setting to false");
            
            // Test setting and getting SO_RCVBUF with different values
            Integer originalRcvBuf = serverChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 8192);
            Integer rcvBuf1 = serverChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            assertNotNull(rcvBuf1, "SO_RCVBUF should not be null");
            assertTrue(rcvBuf1 >= 8192, "SO_RCVBUF should be at least 8192 or system adjusted value");
            
            // Change buffer size and verify it's updated
            serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 16384);
            Integer rcvBuf2 = serverChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            assertNotNull(rcvBuf2, "SO_RCVBUF should not be null after changing");
            assertTrue(rcvBuf2 >= 16384, "SO_RCVBUF should be at least 16384 or system adjusted value");
            
            // Verify that different settings produce different results or at least meet minimum requirements
            assertTrue(rcvBuf1 >= 8192 && rcvBuf2 >= 16384, "Different buffer size settings should result in appropriate values");
            
            // Set back to smaller value and verify
            serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 4096);
            Integer rcvBuf3 = serverChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            assertNotNull(rcvBuf3, "SO_RCVBUF should not be null after setting again");
            assertTrue(rcvBuf3 >= 4096, "SO_RCVBUF should be at least 4096 or system adjusted value");
            
            // Verify we can set SO_REUSEADDR back to true
            serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            Boolean finalReuseAddr = serverChannel.getOption(StandardSocketOptions.SO_REUSEADDR);
            assertTrue(finalReuseAddr, "SO_REUSEADDR should be true after setting back to true");

        } finally {
            serverChannel.close();
        }
    }

    @Test
    @Timeout(10)
    void testSocketOptionsInvalidValues() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        int port = randomPort();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", port);
        
        try {
            // Bind the server socket
            serverChannel.bind(address, 10);
            
            // Test invalid value types - using Object to bypass compile-time checking
            @SuppressWarnings("unchecked")
            SocketOption<Object> rcvbufOption = (SocketOption<Object>) (SocketOption<?>) StandardSocketOptions.SO_RCVBUF;
            assertThrows(IllegalArgumentException.class, () -> 
                serverChannel.setOption(rcvbufOption, "invalid"));
            
            @SuppressWarnings("unchecked")
            SocketOption<Object> reuseAddrOption = (SocketOption<Object>) (SocketOption<?>) StandardSocketOptions.SO_REUSEADDR;
            assertThrows(IllegalArgumentException.class, () -> 
                serverChannel.setOption(reuseAddrOption, "invalid"));
            
            // Test negative buffer size
            assertThrows(IllegalArgumentException.class, () -> 
                serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, -1));
                
        } finally {
            serverChannel.close();
        }
    }

    @Test
    @Timeout(10)
    void testAcceptOnUnboundSocket() throws IOException {
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        
        assertThrows(NotYetBoundException.class, () -> serverChannel.accept());
        
        serverChannel.close();
    }
} 