package dk.ku.di.dms.vms.modb.iouring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import dk.ku.di.dms.vms.modb.iouring.IoUringChannelGroup;
import dk.ku.di.dms.vms.modb.iouring.IoUringServerSocketChannel;
import dk.ku.di.dms.vms.modb.iouring.IoUringSocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Test;


/**
 * Integration test demonstrating the NIO-2 compatibility wrapper usage.
 * This shows how business code can use the standard NIO-2 APIs with io_uring underneath.
 */
public class Nio2IntegrationTest extends TestBase {
    private IoUringChannelGroup group;
    private ThreadFactory threadFactory;

    @BeforeEach
    void setUp() {
        threadFactory = Thread::new;
        group = IoUringChannelGroup.withFixedThreadPool(4, threadFactory);
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
    void testNio2CompatibilityDemo() throws IOException, InterruptedException {
        // This test demonstrates how business code would use the new wrapper classes
        // exactly as they would use standard NIO-2 APIs
        
        int port = randomPort();
        InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", port);
        
        // Create server socket using our wrapper - looks exactly like NIO-2!
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        serverChannel.bind(serverAddress, 10);
        
        CountDownLatch serverAcceptLatch = new CountDownLatch(1);
        AtomicReference<AsynchronousSocketChannel> acceptedChannel = new AtomicReference<>();
        
        // Accept connections using standard NIO-2 CompletionHandler pattern
        serverChannel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Object>() {
            @Override
            public void completed(AsynchronousSocketChannel result, Object attachment) {
                acceptedChannel.set(result);
                serverAcceptLatch.countDown();
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                fail("Server accept failed: " + exc.getMessage());
            }
        });
        
        // Create client socket - also looks exactly like NIO-2!
        IoUringSocketChannel clientChannel = IoUringSocketChannel.open(group);
        
        CountDownLatch clientConnectLatch = new CountDownLatch(1);
        
        // Connect using standard NIO-2 pattern
        clientChannel.connect(serverAddress, null, new CompletionHandler<Void, Object>() {
            @Override
            public void completed(Void result, Object attachment) {
                clientConnectLatch.countDown();
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                fail("Client connect failed: " + exc.getMessage());
            }
        });
        
        // Wait for connection establishment
        assertTrue(clientConnectLatch.await(5, TimeUnit.SECONDS));
        assertTrue(serverAcceptLatch.await(5, TimeUnit.SECONDS));
        assertNotNull(acceptedChannel.get());
        
        // Now test read/write operations
        String testMessage = "Hello, io_uring via NIO-2!";
        ByteBuffer writeBuffer = ByteBuffer.allocateDirect(testMessage.length());
        writeBuffer.put(testMessage.getBytes());
        writeBuffer.flip();
        
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(1024);
        
        CountDownLatch writeLatch = new CountDownLatch(1);
        CountDownLatch readLatch = new CountDownLatch(1);
        CountDownLatch readStartedLatch = new CountDownLatch(1);
        AtomicReference<Integer> bytesWritten = new AtomicReference<>();
        AtomicReference<Integer> bytesRead = new AtomicReference<>();
        
        // Start read operation FIRST to ensure server is ready to receive
        acceptedChannel.get().read(readBuffer, 5000, TimeUnit.MILLISECONDS, null, 
            new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer result, Object attachment) {
                    bytesRead.set(result);
                    readLatch.countDown();
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    fail("Read failed: " + exc.getMessage());
                }
            });
        
        // Signal that read operation has been started
        readStartedLatch.countDown();
        
        // Small delay to ensure read is actually queued
        Thread.sleep(50);
        
        // Now start write operation
        clientChannel.write(writeBuffer, 5000, TimeUnit.MILLISECONDS, null, 
            new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer result, Object attachment) {
                    bytesWritten.set(result);
                    writeLatch.countDown();
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    fail("Write failed: " + exc.getMessage());
                }
            });
        
        // Verify the operations completed successfully
        assertTrue(writeLatch.await(5, TimeUnit.SECONDS));
        assertTrue(readLatch.await(5, TimeUnit.SECONDS));
        
        assertNotNull(bytesWritten.get());
        assertNotNull(bytesRead.get());
        assertTrue(bytesWritten.get() > 0);
        assertTrue(bytesRead.get() > 0);
        
        // Verify the data
        readBuffer.flip();
        byte[] receivedBytes = new byte[bytesRead.get()];
        readBuffer.get(receivedBytes);
        String receivedMessage = new String(receivedBytes);
        assertEquals(testMessage, receivedMessage);
        
        // Clean up
        clientChannel.close();
        acceptedChannel.get().close();
        serverChannel.close();
    }

    @Test
    @Timeout(10)
    void testNio2FutureBasedUsage() throws Exception {
        // This test demonstrates Future-based usage (alternative to CompletionHandler)
        
        int port = randomPort();
        InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", port);
        
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        serverChannel.bind(serverAddress, 10);
        
        IoUringSocketChannel clientChannel = IoUringSocketChannel.open(group);
        
        // Future-based connect
        clientChannel.connect(serverAddress).get(5, TimeUnit.SECONDS);
        
        // Future-based accept  
        AsynchronousSocketChannel acceptedChannel = serverChannel.accept().get(5, TimeUnit.SECONDS);
        assertNotNull(acceptedChannel);
        
        // Future-based read/write
        ByteBuffer writeBuffer = ByteBuffer.allocateDirect(13);
        writeBuffer.put("Hello Future!".getBytes());
        writeBuffer.flip();
        
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(1024);
        
        // These would complete when actual I/O happens
        Integer bytesWritten = clientChannel.write(writeBuffer).get(5, TimeUnit.SECONDS);
        Integer bytesRead = acceptedChannel.read(readBuffer).get(5, TimeUnit.SECONDS);
        
        assertNotNull(bytesWritten);
        assertNotNull(bytesRead);
        assertTrue(bytesWritten > 0);
        assertTrue(bytesRead > 0);
        
        // Clean up
        clientChannel.close();
        acceptedChannel.close();
        serverChannel.close();
    }

    @Test
    @Timeout(20)
    void testMultipleClientsReadWrite() throws IOException, InterruptedException {
        // Test a server handling multiple clients with concurrent read/write operations
        // Uses echo pattern to avoid connection mapping issues
        
        int port = randomPort();
        InetSocketAddress serverAddress = new InetSocketAddress("127.0.0.1", port);
        final int numClients = 3;
        
        // Create server
        IoUringServerSocketChannel serverChannel = IoUringServerSocketChannel.open(group);
        serverChannel.bind(serverAddress, numClients * 2);
        
        // Track accepted connections and set up echo handlers
        CountDownLatch serverAcceptLatch = new CountDownLatch(numClients);
        List<AsynchronousSocketChannel> acceptedChannels = new ArrayList<>();
        
        // Start accepting connections and set up echo for each
        acceptAndEcho(serverChannel, acceptedChannels, serverAcceptLatch);
        
        // Create multiple clients
        IoUringSocketChannel[] clientChannels = new IoUringSocketChannel[numClients];
        CountDownLatch clientConnectLatch = new CountDownLatch(numClients);
        
        for (int i = 0; i < numClients; i++) {
            final int clientIndex = i;
            clientChannels[i] = IoUringSocketChannel.open(group);
            
            clientChannels[i].connect(serverAddress, null, new CompletionHandler<Void, Object>() {
                @Override
                public void completed(Void result, Object attachment) {
                    System.out.println("Client " + clientIndex + ": Connected successfully");
                    clientConnectLatch.countDown();
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    System.err.println("Client " + clientIndex + " connect failed: " + exc.getMessage());
                    exc.printStackTrace();
                    clientConnectLatch.countDown(); // Still count down to avoid hanging
                }
            });
        }
        
        // Wait for all connections
        assertTrue(clientConnectLatch.await(10, TimeUnit.SECONDS), "Client connections timeout");
        assertTrue(serverAcceptLatch.await(10, TimeUnit.SECONDS), "Server accepts timeout");
        
        // Verify all connections are established
        assertEquals(numClients, acceptedChannels.size(), "Not all connections accepted");
        
        // Now test echo operations - each client sends and receives its own message
        CountDownLatch allOperationsLatch = new CountDownLatch(numClients * 2); // write + read per client
        AtomicReference<Integer>[] bytesWritten = new AtomicReference[numClients];
        AtomicReference<String>[] receivedEchoes = new AtomicReference[numClients];
        
        for (int i = 0; i < numClients; i++) {
            bytesWritten[i] = new AtomicReference<>();
            receivedEchoes[i] = new AtomicReference<>();
        }
        
        for (int i = 0; i < numClients; i++) {
            final int clientIndex = i;
            final String clientMessage = "MSG_FROM_CLIENT_" + clientIndex + "_UNIQUE_ID_" + System.nanoTime();
            
            // Client writes unique message
            ByteBuffer writeBuffer = ByteBuffer.allocateDirect(clientMessage.length());
            writeBuffer.put(clientMessage.getBytes());
            writeBuffer.flip();
            
            clientChannels[clientIndex].write(writeBuffer, 5000, TimeUnit.MILLISECONDS, null,
                new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(Integer result, Object attachment) {
                        System.out.println("Client " + clientIndex + ": Wrote " + result + " bytes");
                        bytesWritten[clientIndex].set(result);
                        allOperationsLatch.countDown();
                    }

                    @Override
                    public void failed(Throwable exc, Object attachment) {
                        System.err.println("Client " + clientIndex + " write failed: " + exc.getMessage());
                        exc.printStackTrace();
                        allOperationsLatch.countDown();
                    }
                });
            
            // Client reads echo response
            ByteBuffer readBuffer = ByteBuffer.allocateDirect(1024);
            clientChannels[clientIndex].read(readBuffer, 10000, TimeUnit.MILLISECONDS, null,
                new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(Integer result, Object attachment) {
                        System.out.println("Client " + clientIndex + ": Received echo " + result + " bytes");
                        
                        readBuffer.flip();
                        byte[] echoBytes = new byte[result];
                        readBuffer.get(echoBytes);
                        String echoMessage = new String(echoBytes);
                        receivedEchoes[clientIndex].set(echoMessage);
                        allOperationsLatch.countDown();
                    }

                    @Override
                    public void failed(Throwable exc, Object attachment) {
                        System.err.println("Client " + clientIndex + " read echo failed: " + exc.getMessage());
                        exc.printStackTrace();
                        allOperationsLatch.countDown();
                    }
                });
        }
        
        // Wait for all operations to complete
        assertTrue(allOperationsLatch.await(15, TimeUnit.SECONDS), "Operations timeout");
        
        // Verify all operations completed successfully
        for (int i = 0; i < numClients; i++) {
            assertNotNull(bytesWritten[i].get(), "Client " + i + " write not completed");
            assertTrue(bytesWritten[i].get() > 0, "Client " + i + " wrote 0 bytes");
            
            assertNotNull(receivedEchoes[i].get(), "Client " + i + " did not receive echo");
            
            // Verify echo contains the client's unique identifier
            assertTrue(receivedEchoes[i].get().contains("ECHO:"), 
                "Client " + i + " echo format incorrect: " + receivedEchoes[i].get());
            assertTrue(receivedEchoes[i].get().contains("MSG_FROM_CLIENT_" + i), 
                "Client " + i + " echo doesn't contain client ID: " + receivedEchoes[i].get());
        }
        
        System.out.println("Multiple clients echo test completed successfully!");
        
        // Clean up all connections
        for (int i = 0; i < numClients; i++) {
            if (clientChannels[i] != null) {
                clientChannels[i].close();
            }
        }
        for (AsynchronousSocketChannel channel : acceptedChannels) {
            if (channel != null) {
                channel.close();
            }
        }
        serverChannel.close();
    }
    
    private void acceptAndEcho(IoUringServerSocketChannel serverChannel, 
                             List<AsynchronousSocketChannel> acceptedChannels, 
                             CountDownLatch latch) {
        if (latch.getCount() == 0) {
            return; // All connections accepted
        }
        
        serverChannel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Object>() {
            @Override
            public void completed(AsynchronousSocketChannel clientChannel, Object attachment) {
                synchronized (acceptedChannels) {
                    acceptedChannels.add(clientChannel);
                    int connectionNumber = acceptedChannels.size();
                    System.out.println("Server: Accepted connection " + connectionNumber);
                    
                    // Set up echo handler for this specific client
                    setupEchoForClient(clientChannel, connectionNumber);
                }
                latch.countDown();
                
                // Accept next connection
                acceptAndEcho(serverChannel, acceptedChannels, latch);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                System.err.println("Server accept failed: " + exc.getMessage());
                exc.printStackTrace();
                latch.countDown(); // Still count down to avoid hanging
            }
        });
    }
    
    private void setupEchoForClient(AsynchronousSocketChannel clientChannel, int connectionNumber) {
        // Read from client and echo back
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(1024);
        
        clientChannel.read(readBuffer, 10000, TimeUnit.MILLISECONDS, null,
            new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer result, Object attachment) {
                    if (result > 0) {
                        System.out.println("Server: Read " + result + " bytes from connection " + connectionNumber);
                        
                        // Prepare echo response
                        readBuffer.flip();
                        byte[] receivedBytes = new byte[result];
                        readBuffer.get(receivedBytes);
                        String receivedMessage = new String(receivedBytes);
                        
                        String echoResponse = "ECHO:" + receivedMessage;
                        ByteBuffer echoBuffer = ByteBuffer.allocateDirect(echoResponse.length());
                        echoBuffer.put(echoResponse.getBytes());
                        echoBuffer.flip();
                        
                        // Send echo back
                        clientChannel.write(echoBuffer, 5000, TimeUnit.MILLISECONDS, null,
                            new CompletionHandler<Integer, Object>() {
                                @Override
                                public void completed(Integer writeResult, Object attachment) {
                                    System.out.println("Server: Echoed " + writeResult + " bytes to connection " + connectionNumber);
                                }

                                @Override
                                public void failed(Throwable exc, Object attachment) {
                                    System.err.println("Server echo write failed for connection " + connectionNumber + ": " + exc.getMessage());
                                }
                            });
                    }
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    System.err.println("Server read failed for connection " + connectionNumber + ": " + exc.getMessage());
                    exc.printStackTrace();
                }
            });
    }
} 