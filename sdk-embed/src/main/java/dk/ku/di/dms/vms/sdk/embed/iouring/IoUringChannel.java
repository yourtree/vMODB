/**
 * IoUringChannel.java
 * 
 * Java wrapper for the native io_uring-based socket channel implementation.
 * This class provides a similar interface to Java's AsynchronousSocketChannel
 * but uses Linux's io_uring API for high-performance asynchronous I/O operations.
 */
package dk.ku.di.dms.vms.sdk.embed.iouring;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import dk.ku.di.dms.vms.web_common.channel.IChannel;

/**
 * An asynchronous channel for stream-oriented connecting sockets using Linux's io_uring.
 * This class is a drop-in replacement for {@link java.nio.channels.AsynchronousSocketChannel}.
 */
public class IoUringChannel implements AutoCloseable, NetworkChannel, IChannel {
    
    static {
        NativeLibraryLoader.load();
    }
    
    // Native handle to the C++ IoUringChannel object
    private final long nativeHandle;
    
    // The group this channel belongs to
    private final IoUringChannelGroup group;

    // Local socket address
    private InetSocketAddress localAddress;
    
    // Status constants
    private static final int STATUS_OK = 0;
    private static final int STATUS_ERROR = -1;
    private static final int STATUS_CLOSED = -2;
    
    /**
     * Creates a new io_uring channel.
     * 
     * @param group the channel group
     * @return a new io_uring channel
     * @throws IOException if an I/O error occurs
     */
    public static IoUringChannel open(IoUringChannelGroup group) throws IOException {
        long handle = nativeCreate(group.getNativeHandle());
        if (handle == 0) {
            throw new IOException("Failed to create io_uring channel");
        }
        return new IoUringChannel(handle, group);
    }
    
    /**
     * Constructor.
     * 
     * @param nativeHandle the native handle
     * @param group the channel group
     */
    private IoUringChannel(long nativeHandle, IoUringChannelGroup group) {
        this.nativeHandle = nativeHandle;
        this.group = group;
        nativeSetJavaChannel(nativeHandle, this);
    }
    
    /**
     * Connects this channel to a remote address.
     * 
     * @param remote the remote address to connect to
     * @param attachment the object to attach to the I/O operation
     * @param handler the handler for consuming the result
     * @param <A> the attachment type
     */
    public <A> void connect(SocketAddress remote, A attachment, CompletionHandler<Void, ? super A> handler) {
        if (!(remote instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("Unsupported socket address type: " + remote.getClass().getName());
        }
        
        InetSocketAddress inetAddr = (InetSocketAddress) remote;
        String host = inetAddr.getHostString();
        int port = inetAddr.getPort();
        
        int result = nativeConnect(nativeHandle, host, port, new ConnectCompletionHandler<>(handler, attachment));
        if (result != STATUS_OK) {
            throw new IllegalStateException("Failed to initiate connect operation");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> connect(InetSocketAddress remote) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        connect(remote, null, new CompletionHandler<Void, Void>() {
            @Override
            public void completed(Void result, Void attachment) {
                future.complete(null);
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                future.completeExceptionally(exc);
            }
        });
        return future;
    }
    
    /**
     * Accepts an incoming connection.
     * 
     * @param attachment the object to attach to the I/O operation
     * @param handler the handler for consuming the result
     * @param <A> the attachment type
     */
    public <A> void accept(A attachment, CompletionHandler<IoUringChannel, ? super A> handler) {
        int result = nativeAccept(nativeHandle, new AcceptCompletionHandler<>(handler, attachment));
        if (result != STATUS_OK) {
            throw new IllegalStateException("Failed to initiate accept operation");
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        read(dst, 0, attachment, handler);
    }
    
    /**
     * Reads data from this channel into the given buffer, with position.
     * 
     * @param dst the buffer into which bytes are to be transferred
     * @param position the position in the file from which bytes are to be transferred
     * @param attachment the object to attach to the I/O operation
     * @param handler the handler for consuming the result
     * @param <A> the attachment type
     */
    public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        if (!dst.isDirect()) {
            throw new IllegalArgumentException("Only direct buffers are supported");
        }
        
        int result = nativeRead(nativeHandle, dst, position, dst.remaining(), handler);
        if (result != STATUS_OK) {
            throw new IllegalStateException("Failed to initiate read operation");
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Integer> write(ByteBuffer src) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        write(src, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                future.complete(result);
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                future.completeExceptionally(exc);
            }
        });
        return future;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        write(src, attachment, handler);
    }
    
    /**
     * Writes data from the given buffer to this channel.
     * 
     * @param src the buffer from which bytes are to be transferred
     * @param attachment the object to attach to the I/O operation
     * @param handler the handler for consuming the result
     * @param <A> the attachment type
     */
    public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
        write(src, 0, attachment, handler);
    }
    
    /**
     * Writes data from the given buffer to this channel, with position.
     * 
     * @param src the buffer from which bytes are to be transferred
     * @param position the position in the file at which the transfer is to begin
     * @param attachment the object to attach to the I/O operation
     * @param handler the handler for consuming the result
     * @param <A> the attachment type
     */
    public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        if (!src.isDirect()) {
            throw new IllegalArgumentException("Only direct buffers are supported");
        }
        
        int result = nativeWrite(nativeHandle, src, position, src.remaining(), handler);
        if (result != STATUS_OK) {
            throw new IllegalStateException("Failed to initiate write operation");
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IoUringChannel bind(SocketAddress local) throws IOException {
        if (!(local instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("Unsupported socket address type: " + local.getClass().getName());
        }
        
        this.localAddress = (InetSocketAddress) local;
        // In a real implementation, we'd call native code to bind the socket
        // But for now, let's just store the local address
        return this;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public <T> IoUringChannel setOption(SocketOption<T> name, T value) throws IOException {
        // Implement socket options as needed
        return this;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        // Implement socket options as needed
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<SocketOption<?>> supportedOptions() {
        // Return supported socket options
        return Set.of();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return nativeIsOpen(nativeHandle);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        int result = nativeClose(nativeHandle);
        if (result != STATUS_OK && result != STATUS_CLOSED) {
            throw new IOException("Failed to close io_uring channel");
        }
        nativeDestroy(nativeHandle);
    }
    
    /**
     * Closes this channel without throwing IOException.
     * This method is provided to conform to the IChannel interface.
     */
    public void closeQuietly() {
        try {
            close();
        } catch (IOException ignored) {
            // Ignore exceptions as per IChannel contract
        }
    }
    
    /**
     * Gets the remote socket address to which this channel is connected.
     * 
     * @return the remote socket address, or null if the channel is not connected
     * @throws IOException if an I/O error occurs
     */
    public SocketAddress getRemoteAddress() throws IOException {
        String address = nativeGetRemoteAddress(nativeHandle);
        if (address == null || address.isEmpty()) {
            return null;
        }
        return IoUringUtils.parseSocketAddress(address);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return this.localAddress;
    }
    
    /**
     * Configures socket options for the channel.
     * 
     * @param soRcvBuf the socket receive buffer size
     * @param soSndBuf the socket send buffer size
     * @throws IOException if an I/O error occurs
     */
    public void configureSocket(int soRcvBuf, int soSndBuf) throws IOException {
        int result = nativeConfigureSocket(nativeHandle, soRcvBuf, soSndBuf);
        if (result != STATUS_OK) {
            throw new IOException("Failed to configure socket options");
        }
    }
    
    private static class ConnectCompletionHandler<A> implements CompletionHandler<Integer, Void> {
        private final CompletionHandler<Void, ? super A> handler;
        private final A attachment;
        
        ConnectCompletionHandler(CompletionHandler<Void, ? super A> handler, A attachment) {
            this.handler = handler;
            this.attachment = attachment;
        }
        
        @Override
        public void completed(Integer result, Void attachment) {
            handler.completed(null, this.attachment);
        }
        
        @Override
        public void failed(Throwable exc, Void attachment) {
            handler.failed(exc, this.attachment);
        }
    }
    
    private static class AcceptCompletionHandler<A> implements CompletionHandler<IoUringChannel, Void> {
        private final CompletionHandler<IoUringChannel, ? super A> handler;
        private final A attachment;
        
        AcceptCompletionHandler(CompletionHandler<IoUringChannel, ? super A> handler, A attachment) {
            this.handler = handler;
            this.attachment = attachment;
        }
        
        @Override
        public void completed(IoUringChannel result, Void attachment) {
            handler.completed(result, this.attachment);
        }
        
        @Override
        public void failed(Throwable exc, Void attachment) {
            handler.failed(exc, this.attachment);
        }
    }
    
    private static native long nativeCreate(long groupHandle);
    private static native long nativeFromFileDescriptor(int fd, long groupHandle);
    private static native void nativeDestroy(long handle);
    
    private native int nativeConnect(long handle, String host, int port, CompletionHandler<Integer, Void> handler);
    private native int nativeAccept(long handle, CompletionHandler<IoUringChannel, Void> handler);
    private native int nativeRead(long handle, ByteBuffer buffer, long position, int length, CompletionHandler<Integer, ?> handler);
    private native int nativeWrite(long handle, ByteBuffer buffer, long position, int length, CompletionHandler<Integer, ?> handler);
    private native int nativeClose(long handle);
    private native boolean nativeIsOpen(long handle);
    private native int nativeConfigureSocket(long handle, int soRcvBuf, int soSndBuf);
    private native String nativeGetRemoteAddress(long handle);
    private native void nativeSetJavaChannel(long handle, IoUringChannel channel);
} 