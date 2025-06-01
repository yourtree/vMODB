package dk.ku.di.dms.vms.modb.iouring;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * AsynchronousSocketChannel implementation using io_uring.
 */
public final class IoUringSocketChannel extends AsynchronousSocketChannel {
    private final IoUringChannelGroup group;
    private IoUringSocket nativeSocket;
    private volatile boolean closed = false;
    
    // Internal constructor for server-side accepted sockets
    IoUringSocketChannel(IoUringChannelGroup group, IoUringSocket nativeSocket) {
        super(null);
        this.group = group;
        this.nativeSocket = nativeSocket;
    }

    // Constructor for client-side sockets
    private IoUringSocketChannel(IoUringChannelGroup group) {
        super(null);
        this.group = group;
        this.nativeSocket = null; // Will be created in connect
    }

    /**
     * Opens a new IoUringSocketChannel.
     * 
     * @param group the channel group
     * @return new socket channel
     */
    public static IoUringSocketChannel open(IoUringChannelGroup group) {
        return new IoUringSocketChannel(group);
    }

    /**
     * Opens a new IoUringSocketChannel with a default group.
     * 
     * @return new socket channel
     */
    public static IoUringSocketChannel open() {
        IoUringChannelGroup defaultGroup = IoUringChannelGroup.withFixedThreadPool(1, Thread::new);
        return new IoUringSocketChannel(defaultGroup);
    }

    @Override
    public <A> void connect(SocketAddress remote, A attachment, CompletionHandler<Void, ? super A> handler) {
        if (nativeSocket != null) {
            group.executor().execute(() -> 
                handler.failed(new IllegalStateException("Already connected"), attachment));
            return;
        }

        if (!(remote instanceof InetSocketAddress)) {
            group.executor().execute(() -> 
                handler.failed(new UnsupportedOperationException("Only InetSocketAddress is supported"), attachment));
            return;
        }

        InetSocketAddress inet = (InetSocketAddress) remote;
        IoUringSocket socket = new IoUringSocket(inet.getAddress().getHostAddress(), inet.getPort());
        this.nativeSocket = socket; // Store the socket

        socket.onConnect(ioUring -> {
            group.executor().execute(() -> handler.completed(null, attachment));
        });

        socket.onException(exception -> {
            group.executor().execute(() -> handler.failed(exception, attachment));
        });

        group.ring().queueConnect(socket);
    }

    @Override
    public Future<Void> connect(SocketAddress remote) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        connect(remote, null, new CompletionHandler<Void, Object>() {
            @Override
            public void completed(Void result, Object attachment) {
                future.complete(null);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    @Override
    public <A> void read(ByteBuffer dst, long timeout, TimeUnit unit, A attachment, 
                        CompletionHandler<Integer, ? super A> handler) {

        if (dst.remaining() == 0) {
            group.executor().execute(() -> handler.completed(0, attachment));
            return;
        }
        if (!dst.isDirect()) {
            group.executor().execute(() -> 
                handler.failed(new IllegalArgumentException("Buffer must be direct"), attachment));
            return;
        }

        if (nativeSocket == null) {
            group.executor().execute(() -> 
                handler.failed(new IllegalStateException("Not connected"), attachment));
            return;
        }

        // Store initial state
        final int initialPos = dst.position();
        final int initialLimit = dst.limit();
        
        // Create a sliced buffer for io_uring (position=0)
        // AbstractIoUringChannel expects buffer to start at position 0
        final ByteBuffer ioBuffer = dst.duplicate();
        ioBuffer.position(initialPos);
        final ByteBuffer slicedBuffer = ioBuffer.slice();
        
        nativeSocket.onRead(buffer -> {
            // buffer is slicedBuffer
            // AbstractIoUringChannel has called: buffer.position(buffer.position() + bytesRead)
            // Since slicedBuffer started at position 0, bytesRead = current position
            int bytesRead = buffer.position();
            
            if (bytesRead <= 0) {
                System.err.printf("[IoUringSocketChannel.read] EOF or error: bytesRead=%d%n", bytesRead);
            }
            
            System.err.printf("[IoUringSocketChannel.read] Read complete: bytesRead=%d, buffer.pos=%d, buffer.limit=%d%n",
                bytesRead, buffer.position(), buffer.limit());
            
            // Update the original buffer's position on the executor thread
            group.executor().execute(() -> {
                if (bytesRead > 0) {
                    try {
                        // Check current buffer state
                        int currentPos = dst.position();
                        int currentLimit = dst.limit();
                        
                        System.err.printf("[IoUringSocketChannel.read] Updating dst: currentPos=%d, currentLimit=%d, bytesRead=%d%n",
                            currentPos, currentLimit, bytesRead);
                        
                        // Update original buffer position if it hasn't been modified
                        int originalPosition = initialPos;
                        if (currentPos == originalPosition) {
                            // System.out.println("[IoUringSocketChannel.read] Updating dst: currentPos=" + currentPos + 
                            //         ", currentLimit=" + currentLimit + ", bytesRead=" + bytesRead);
                            try {
                                dst.position(currentPos + bytesRead);
                                // System.out.println("[IoUringSocketChannel.read] Updated position to " + dst.position());
                            } catch (IllegalArgumentException e) {
                                System.err.println("[IoUringSocketChannel.read] Failed to update position: " + e.getMessage());
                                System.err.println("[IoUringSocketChannel.read] Buffer state - position: " + currentPos + 
                                        ", limit: " + currentLimit + ", capacity: " + dst.capacity() + 
                                        ", bytesRead: " + bytesRead);
                                e.printStackTrace();
                                // Re-throw to let completion handler deal with it
                                throw e;
                            }
                        } else {
                            // System.out.println("[IoUringSocketChannel.read] Position changed externally: " +
                            //         "original=" + originalPosition + ", current=" + currentPos);
                        }
                    } catch (Exception e) {
                        System.err.printf("[IoUringSocketChannel.read] Exception: %s%n", e);
                        e.printStackTrace();
                    }
                }
                handler.completed(bytesRead <= 0 ? -1 : bytesRead, attachment);
            });
        });

        nativeSocket.onException(exception -> {
            group.executor().execute(() -> handler.failed(exception, attachment));
        });

        // Use the sliced buffer for io_uring operations
        group.ring().queueRead(nativeSocket, slicedBuffer);
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        read(dst, 0L, TimeUnit.MILLISECONDS, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                future.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, 
                         CompletionHandler<Integer, ? super A> handler) {
        System.err.printf("[IoUringSocketChannel.write] Write requested: %d bytes%n", src.remaining());
        if (src.remaining() == 0) {
            group.executor().execute(() -> handler.completed(0, attachment));
            return;
        }
        if (!src.isDirect()) {
            group.executor().execute(() -> 
                handler.failed(new IllegalArgumentException("Buffer must be direct"), attachment));
            return;
        }

        if (nativeSocket == null) {
            group.executor().execute(() -> 
                handler.failed(new IllegalStateException("Not connected"), attachment));
            return;
        }
        
        // Capture initial state
        final int initialPos = src.position();
        final int bytesToWrite = src.remaining();
        
        // Advance position immediately (AsynchronousSocketChannel contract)
        src.position(src.limit());
        
        nativeSocket.onWrite(buffer -> {
            int bytesWritten = buffer.position();
            System.err.printf("[IoUringSocketChannel.write] Write completed: %d bytes (expected %d)%n", 
                bytesWritten, bytesToWrite);
            
            if (bytesWritten < bytesToWrite) {
                final int finalBytesWritten = bytesWritten;
                group.executor().execute(() -> {
                    src.position(initialPos + finalBytesWritten);
                    handler.completed(finalBytesWritten, attachment);
                });
            } else {
                group.executor().execute(() -> handler.completed(bytesToWrite, attachment));
            }
        });

        nativeSocket.onException(exception -> {
            group.executor().execute(() -> {
                try {
                    src.position(initialPos);
                } catch (Exception ignored) {}
                handler.failed(exception, attachment);
            });
        });

        // Use the original buffer directly if it's direct, otherwise copy
        if (src.isDirect()) {
            ByteBuffer writeView = src.duplicate();
            writeView.position(initialPos);
            writeView.limit(initialPos + bytesToWrite);
            group.ring().queueWrite(nativeSocket, writeView);
        } else {
            // Should not happen as we check isDirect() earlier
            group.executor().execute(() -> 
                handler.failed(new IllegalArgumentException("Buffer must be direct"), attachment));
        }
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        write(src, 0L, TimeUnit.MILLISECONDS, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                future.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    @Override
    public boolean isOpen() {
        return !closed && (nativeSocket == null || nativeSocket.isOpen());
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (nativeSocket != null) {
                nativeSocket.close();
            }
        }
    }

    protected void implCloseChannel() throws IOException {
        // Implementation for internal use
        close();
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        if (nativeSocket == null) {
            return null;
        }
        return new InetSocketAddress(nativeSocket.ipAddress(), nativeSocket.port());
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        if (nativeSocket == null) {
            return null;
        }
        return new InetSocketAddress(nativeSocket.ipAddress(), nativeSocket.port());
    }

    // Unsupported operations
    @Override
    public AsynchronousSocketChannel bind(SocketAddress local) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> AsynchronousSocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException("Socket option cannot be null");
        }
        
        if (closed || nativeSocket == null) {
            throw new ClosedChannelException();
        }
        
        if (!supportedOptions().contains(name)) {
            throw new UnsupportedOperationException("Socket option " + name + " is not supported");
        }
        
        try {
            if (name == StandardSocketOptions.SO_SNDBUF) {
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("SO_SNDBUF expects Integer value");
                }
                Integer bufSize = (Integer) value;
                if (bufSize < 0) {
                    throw new IllegalArgumentException("Buffer size cannot be negative");
                }
                nativeSocket.setSocketOption(1, bufSize);
            } else if (name == StandardSocketOptions.SO_RCVBUF) {
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("SO_RCVBUF expects Integer value");
                }
                Integer bufSize = (Integer) value;
                if (bufSize < 0) {
                    throw new IllegalArgumentException("Buffer size cannot be negative");
                }
                nativeSocket.setSocketOption(2, bufSize);
            } else if (name == StandardSocketOptions.SO_KEEPALIVE) {
                if (!(value instanceof Boolean)) {
                    throw new IllegalArgumentException("SO_KEEPALIVE expects Boolean value");
                }
                Boolean keepAlive = (Boolean) value;
                nativeSocket.setSocketOption(3, keepAlive ? 1 : 0);
            } else if (name == StandardSocketOptions.SO_REUSEADDR) {
                if (!(value instanceof Boolean)) {
                    throw new IllegalArgumentException("SO_REUSEADDR expects Boolean value");
                }
                Boolean reuseAddr = (Boolean) value;
                nativeSocket.setSocketOption(4, reuseAddr ? 1 : 0);
            } else if (name == StandardSocketOptions.TCP_NODELAY) {
                if (!(value instanceof Boolean)) {
                    throw new IllegalArgumentException("TCP_NODELAY expects Boolean value");
                }
                Boolean noDelay = (Boolean) value;
                nativeSocket.setSocketOption(5, noDelay ? 1 : 0);
            }
        } catch (IllegalArgumentException e) {
            // Re-throw IllegalArgumentException as-is
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to set socket option " + name, e);
        }
        
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getOption(SocketOption<T> name) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException("Socket option cannot be null");
        }
        
        if (closed || nativeSocket == null) {
            throw new ClosedChannelException();
        }
        
        if (!supportedOptions().contains(name)) {
            throw new UnsupportedOperationException("Socket option " + name + " is not supported");
        }
        
        try {
            if (name == StandardSocketOptions.SO_SNDBUF) {
                return (T) Integer.valueOf(nativeSocket.getSocketOption(1));
            } else if (name == StandardSocketOptions.SO_RCVBUF) {
                return (T) Integer.valueOf(nativeSocket.getSocketOption(2));
            } else if (name == StandardSocketOptions.SO_KEEPALIVE) {
                return (T) Boolean.valueOf(nativeSocket.getSocketOption(3) != 0);
            } else if (name == StandardSocketOptions.SO_REUSEADDR) {
                return (T) Boolean.valueOf(nativeSocket.getSocketOption(4) != 0);
            } else if (name == StandardSocketOptions.TCP_NODELAY) {
                return (T) Boolean.valueOf(nativeSocket.getSocketOption(5) != 0);
            }
        } catch (Exception e) {
            throw new IOException("Failed to get socket option " + name, e);
        }
        
        return null;
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        Set<SocketOption<?>> options = new HashSet<>();
        options.add(StandardSocketOptions.SO_SNDBUF);
        options.add(StandardSocketOptions.SO_RCVBUF);
        options.add(StandardSocketOptions.SO_KEEPALIVE);
        options.add(StandardSocketOptions.SO_REUSEADDR);
        options.add(StandardSocketOptions.TCP_NODELAY);
        return Collections.unmodifiableSet(options);
    }

    @Override
    public AsynchronousSocketChannel shutdownInput() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsynchronousSocketChannel shutdownOutput() throws IOException {
        throw new UnsupportedOperationException();
    }

    // Additional required methods for AsynchronousSocketChannel
    @Override
    public <A> void read(ByteBuffer[] dsts, int offset, int length, long timeout, TimeUnit unit, A attachment, 
                        CompletionHandler<Long, ? super A> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void write(ByteBuffer[] srcs, int offset, int length, long timeout, TimeUnit unit, A attachment, 
                         CompletionHandler<Long, ? super A> handler) {
        throw new UnsupportedOperationException();
    }
} 