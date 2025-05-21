package dk.ku.di.dms.vms.sdk.embed.iouring;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class IoUringChannelTest {
    private IoUringChannelGroup channelGroup;
    private static final int TEST_PORT = 12345;
    private static final String TEST_HOST = "localhost";
    private static final int BUFFER_SIZE = 1024;

    @BeforeEach
    public void setUp() {
        channelGroup = IoUringChannelGroup.create(1024, 2);
        assertNotNull(channelGroup, "Channel group should be created");
    }

    @AfterEach
    public void tearDown() {
        if (channelGroup != null) {
            channelGroup.shutdown();
        }
    }

    @Test
    public void testConnect() throws Exception {
        // Create server socket
        IoUringChannel serverChannel = IoUringChannel.create(channelGroup);
        serverChannel.bind(new InetSocketAddress(TEST_PORT));
        serverChannel.listen(1);

        // Create client channel
        IoUringChannel clientChannel = IoUringChannel.create(channelGroup);
        
        CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        clientChannel.connect(TEST_HOST, TEST_PORT, new CompletionHandler<Void>() {
            @Override
            public void completed(Void result, Object attachment) {
                connectFuture.complete(null);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                connectFuture.completeExceptionally(exc);
            }
        });

        // Wait for connection
        connectFuture.get(5, TimeUnit.SECONDS);
        assertTrue(clientChannel.isConnected(), "Client should be connected");
        
        // Cleanup
        clientChannel.close();
        serverChannel.close();
    }

    @Test
    public void testReadWrite() throws Exception {
        // Create server socket
        IoUringChannel serverChannel = IoUringChannel.create(channelGroup);
        serverChannel.bind(new InetSocketAddress(TEST_PORT));
        serverChannel.listen(1);

        // Create client channel
        IoUringChannel clientChannel = IoUringChannel.create(channelGroup);
        
        // Connect
        CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        clientChannel.connect(TEST_HOST, TEST_PORT, new CompletionHandler<Void>() {
            @Override
            public void completed(Void result, Object attachment) {
                connectFuture.complete(null);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                connectFuture.completeExceptionally(exc);
            }
        });
        connectFuture.get(5, TimeUnit.SECONDS);

        // Accept on server
        CompletableFuture<IoUringChannel> acceptFuture = new CompletableFuture<>();
        serverChannel.accept(new CompletionHandler<IoUringChannel>() {
            @Override
            public void completed(IoUringChannel result, Object attachment) {
                acceptFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                acceptFuture.completeExceptionally(exc);
            }
        });
        IoUringChannel acceptedChannel = acceptFuture.get(5, TimeUnit.SECONDS);

        // Write data
        String testMessage = "Hello, io_uring!";
        ByteBuffer writeBuffer = ByteBuffer.wrap(testMessage.getBytes());
        CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
        clientChannel.write(writeBuffer, new CompletionHandler<Integer>() {
            @Override
            public void completed(Integer result, Object attachment) {
                writeFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                writeFuture.completeExceptionally(exc);
            }
        });
        int bytesWritten = writeFuture.get(5, TimeUnit.SECONDS);
        assertEquals(testMessage.length(), bytesWritten, "Should write all bytes");

        // Read data
        ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        CompletableFuture<Integer> readFuture = new CompletableFuture<>();
        acceptedChannel.read(readBuffer, new CompletionHandler<Integer>() {
            @Override
            public void completed(Integer result, Object attachment) {
                readFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                readFuture.completeExceptionally(exc);
            }
        });
        int bytesRead = readFuture.get(5, TimeUnit.SECONDS);
        assertEquals(testMessage.length(), bytesRead, "Should read all bytes");
        
        // Verify data
        readBuffer.flip();
        byte[] receivedData = new byte[bytesRead];
        readBuffer.get(receivedData);
        assertEquals(testMessage, new String(receivedData), "Data should match");

        // Cleanup
        clientChannel.close();
        acceptedChannel.close();
        serverChannel.close();
    }

    @Test
    public void testClose() throws Exception {
        IoUringChannel channel = IoUringChannel.create(channelGroup);
        assertTrue(channel.isOpen(), "Channel should be open");
        
        channel.close();
        assertFalse(channel.isOpen(), "Channel should be closed");
    }

    @Test
    public void testConfigureSocket() throws Exception {
        IoUringChannel channel = IoUringChannel.create(channelGroup);
        
        // Test socket configuration
        channel.configureSocket(8192, 8192); // Set receive and send buffer sizes
        
        // Test blocking mode
        channel.configureBlocking(true);
        assertTrue(channel.isBlocking(), "Channel should be in blocking mode");
        
        channel.configureBlocking(false);
        assertFalse(channel.isBlocking(), "Channel should be in non-blocking mode");
        
        channel.close();
    }

    @Test
    public void testAccept() throws Exception {
        // Create server socket
        IoUringChannel serverChannel = IoUringChannel.create(channelGroup);
        serverChannel.bind(new InetSocketAddress(TEST_PORT));
        serverChannel.listen(1);

        // Create client channel
        IoUringChannel clientChannel = IoUringChannel.create(channelGroup);
        
        // Start accepting before connecting
        CompletableFuture<IoUringChannel> acceptFuture = new CompletableFuture<>();
        serverChannel.accept(new CompletionHandler<IoUringChannel>() {
            @Override
            public void completed(IoUringChannel result, Object attachment) {
                acceptFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                acceptFuture.completeExceptionally(exc);
            }
        });

        // Connect client
        CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        clientChannel.connect(TEST_HOST, TEST_PORT, new CompletionHandler<Void>() {
            @Override
            public void completed(Void result, Object attachment) {
                connectFuture.complete(null);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                connectFuture.completeExceptionally(exc);
            }
        });

        // Wait for both operations
        connectFuture.get(5, TimeUnit.SECONDS);
        IoUringChannel acceptedChannel = acceptFuture.get(5, TimeUnit.SECONDS);

        // Verify accepted channel
        assertNotNull(acceptedChannel, "Accepted channel should not be null");
        assertTrue(acceptedChannel.isOpen(), "Accepted channel should be open");
        assertTrue(acceptedChannel.isConnected(), "Accepted channel should be connected");

        // Cleanup
        clientChannel.close();
        acceptedChannel.close();
        serverChannel.close();
    }

    @Test
    public void testWrite() throws Exception {
        // Create server socket
        IoUringChannel serverChannel = IoUringChannel.create(channelGroup);
        serverChannel.bind(new InetSocketAddress(TEST_PORT));
        serverChannel.listen(1);

        // Create client channel
        IoUringChannel clientChannel = IoUringChannel.create(channelGroup);
        
        // Connect
        CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        clientChannel.connect(TEST_HOST, TEST_PORT, new CompletionHandler<Void>() {
            @Override
            public void completed(Void result, Object attachment) {
                connectFuture.complete(null);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                connectFuture.completeExceptionally(exc);
            }
        });
        connectFuture.get(5, TimeUnit.SECONDS);

        // Accept on server
        CompletableFuture<IoUringChannel> acceptFuture = new CompletableFuture<>();
        serverChannel.accept(new CompletionHandler<IoUringChannel>() {
            @Override
            public void completed(IoUringChannel result, Object attachment) {
                acceptFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                acceptFuture.completeExceptionally(exc);
            }
        });
        IoUringChannel acceptedChannel = acceptFuture.get(5, TimeUnit.SECONDS);

        // Write data
        String testMessage = "Hello, io_uring!";
        ByteBuffer writeBuffer = ByteBuffer.wrap(testMessage.getBytes());
        CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
        clientChannel.write(writeBuffer, new CompletionHandler<Integer>() {
            @Override
            public void completed(Integer result, Object attachment) {
                writeFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                writeFuture.completeExceptionally(exc);
            }
        });
        int bytesWritten = writeFuture.get(5, TimeUnit.SECONDS);
        assertEquals(testMessage.length(), bytesWritten, "Should write all bytes");

        // Cleanup
        clientChannel.close();
        acceptedChannel.close();
        serverChannel.close();
    }

    @Test
    public void testRead() throws Exception {
        // Create server socket
        IoUringChannel serverChannel = IoUringChannel.create(channelGroup);
        serverChannel.bind(new InetSocketAddress(TEST_PORT));
        serverChannel.listen(1);

        // Create client channel
        IoUringChannel clientChannel = IoUringChannel.create(channelGroup);
        
        // Connect
        CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        clientChannel.connect(TEST_HOST, TEST_PORT, new CompletionHandler<Void>() {
            @Override
            public void completed(Void result, Object attachment) {
                connectFuture.complete(null);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                connectFuture.completeExceptionally(exc);
            }
        });
        connectFuture.get(5, TimeUnit.SECONDS);

        // Accept on server
        CompletableFuture<IoUringChannel> acceptFuture = new CompletableFuture<>();
        serverChannel.accept(new CompletionHandler<IoUringChannel>() {
            @Override
            public void completed(IoUringChannel result, Object attachment) {
                acceptFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                acceptFuture.completeExceptionally(exc);
            }
        });
        IoUringChannel acceptedChannel = acceptFuture.get(5, TimeUnit.SECONDS);

        // Write data from client
        String testMessage = "Hello, io_uring!";
        ByteBuffer writeBuffer = ByteBuffer.wrap(testMessage.getBytes());
        CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
        clientChannel.write(writeBuffer, new CompletionHandler<Integer>() {
            @Override
            public void completed(Integer result, Object attachment) {
                writeFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                writeFuture.completeExceptionally(exc);
            }
        });
        writeFuture.get(5, TimeUnit.SECONDS);

        // Read data on server
        ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        CompletableFuture<Integer> readFuture = new CompletableFuture<>();
        acceptedChannel.read(readBuffer, new CompletionHandler<Integer>() {
            @Override
            public void completed(Integer result, Object attachment) {
                readFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                readFuture.completeExceptionally(exc);
            }
        });
        int bytesRead = readFuture.get(5, TimeUnit.SECONDS);
        assertEquals(testMessage.length(), bytesRead, "Should read all bytes");
        
        // Verify data
        readBuffer.flip();
        byte[] receivedData = new byte[bytesRead];
        readBuffer.get(receivedData);
        assertEquals(testMessage, new String(receivedData), "Data should match");

        // Cleanup
        clientChannel.close();
        acceptedChannel.close();
        serverChannel.close();
    }

    @Test
    public void testLargeDataTransfer() throws Exception {
        // Create server socket
        IoUringChannel serverChannel = IoUringChannel.create(channelGroup);
        serverChannel.bind(new InetSocketAddress(TEST_PORT));
        serverChannel.listen(1);

        // Create client channel
        IoUringChannel clientChannel = IoUringChannel.create(channelGroup);
        
        // Connect
        CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        clientChannel.connect(TEST_HOST, TEST_PORT, new CompletionHandler<Void>() {
            @Override
            public void completed(Void result, Object attachment) {
                connectFuture.complete(null);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                connectFuture.completeExceptionally(exc);
            }
        });
        connectFuture.get(5, TimeUnit.SECONDS);

        // Accept on server
        CompletableFuture<IoUringChannel> acceptFuture = new CompletableFuture<>();
        serverChannel.accept(new CompletionHandler<IoUringChannel>() {
            @Override
            public void completed(IoUringChannel result, Object attachment) {
                acceptFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                acceptFuture.completeExceptionally(exc);
            }
        });
        IoUringChannel acceptedChannel = acceptFuture.get(5, TimeUnit.SECONDS);

        // Create large data (1MB)
        byte[] largeData = new byte[1024 * 1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        // Write large data
        ByteBuffer writeBuffer = ByteBuffer.wrap(largeData);
        CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
        clientChannel.write(writeBuffer, new CompletionHandler<Integer>() {
            @Override
            public void completed(Integer result, Object attachment) {
                writeFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                writeFuture.completeExceptionally(exc);
            }
        });
        int bytesWritten = writeFuture.get(5, TimeUnit.SECONDS);
        assertEquals(largeData.length, bytesWritten, "Should write all bytes");

        // Read large data
        ByteBuffer readBuffer = ByteBuffer.allocate(largeData.length);
        CompletableFuture<Integer> readFuture = new CompletableFuture<>();
        acceptedChannel.read(readBuffer, new CompletionHandler<Integer>() {
            @Override
            public void completed(Integer result, Object attachment) {
                readFuture.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                readFuture.completeExceptionally(exc);
            }
        });
        int bytesRead = readFuture.get(5, TimeUnit.SECONDS);
        assertEquals(largeData.length, bytesRead, "Should read all bytes");

        // Verify data
        readBuffer.flip();
        byte[] receivedData = new byte[bytesRead];
        readBuffer.get(receivedData);
        assertArrayEquals(largeData, receivedData, "Large data should match exactly");

        // Cleanup
        clientChannel.close();
        acceptedChannel.close();
        serverChannel.close();
    }
} 