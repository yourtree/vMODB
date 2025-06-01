package dk.ku.di.dms.vms.modb.iouring;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.channels.AcceptPendingException;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NotYetBoundException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * AsynchronousServerSocketChannel implementation using io_uring.
 */
public final class IoUringServerSocketChannel extends AsynchronousServerSocketChannel {
    private final IoUringChannelGroup group;
    private IoUringServerSocket nativeSocket;
    private volatile boolean closed = false;

    private volatile boolean bound = false;
    private final AtomicBoolean acceptPending = new AtomicBoolean(false);

    private IoUringServerSocketChannel(IoUringChannelGroup group) {
        super(null);
        this.group = group;
    }

    /**
     * Opens a new IoUringServerSocketChannel.
     * 
     * @param group the channel group
     * @return new server socket channel
     */
    public static IoUringServerSocketChannel open(IoUringChannelGroup group) {
        return new IoUringServerSocketChannel(group);
    }

    /**
     * Opens a new IoUringServerSocketChannel with a default group.
     * 
     * @return new server socket channel
     */
    public static IoUringServerSocketChannel open() {
        IoUringChannelGroup defaultGroup = IoUringChannelGroup.withFixedThreadPool(1, Thread::new);
        return new IoUringServerSocketChannel(defaultGroup);
    }

    @Override
    public AsynchronousServerSocketChannel bind(SocketAddress local, int backlog) throws IOException {


        if (closed) throw new ClosedChannelException();
        if (bound)  throw new AlreadyBoundException();
        if (backlog < 0) throw new IllegalArgumentException("backlog < 0");

        if (local == null) local = new InetSocketAddress(0);

        if (!(local instanceof InetSocketAddress)) {
            throw new UnsupportedOperationException("Only InetSocketAddress is supported");
        }
        
        InetSocketAddress inet = (InetSocketAddress) local;
        String address = inet.getAddress().getHostAddress();
        int port = inet.getPort();
        
        this.nativeSocket = new IoUringServerSocket(address, port, backlog);
        bound = true;
        return this;
    }

    @Override
    public <A> void accept(A attachment, CompletionHandler<AsynchronousSocketChannel, ? super A> handler) {
        if (handler == null) throw new NullPointerException();
        if (!bound)   throw new NotYetBoundException();
        if (closed)   throw new IllegalStateException("Socket closed");
        if (!acceptPending.compareAndSet(false,true))
            throw new AcceptPendingException();
        
        if (nativeSocket == null) {
            group.executor().execute(() -> 
                handler.failed(new IllegalStateException("Socket not bound"), attachment));
            return;
        }

        nativeSocket.onAccept((ioUring, socket) -> {
            /*
             * Reset the flag _before_ we dispatch the callback to the user supplied
             * CompletionHandler.  The callback is executed on a thread from the
             * ChannelGroup executor which may start running immediately. If we leave
             * the flag set until **after** the callback is scheduled we create a
             * window where the user code can call accept(..) again while
             * `acceptPending` is still true, leading to an unexpected
             * AcceptPendingException. By clearing it first we guarantee that the
             * next accept call can always proceed.
             */
            acceptPending.set(false);

            if (closed) {
                group.executor().execute(() -> handler.failed(new AsynchronousCloseException(), attachment));
                return;
            }

            IoUringSocketChannel channel = new IoUringSocketChannel(group, socket);
            group.executor().execute(() -> handler.completed(channel, attachment));
        });

        nativeSocket.onException(exception -> {
            /*
             * Same reasoning as above – clear the flag first so the user can
             * issue a new accept even if this one fails.
             */
            acceptPending.set(false);
            group.executor().execute(() -> handler.failed(exception, attachment));
        });

        group.ring().queueAccept(nativeSocket);
    }

    @Override
    public Future<AsynchronousSocketChannel> accept() {
        CompletableFuture<AsynchronousSocketChannel> future = new CompletableFuture<>();
        accept(null, new CompletionHandler<AsynchronousSocketChannel, Object>() {
            @Override
            public void completed(AsynchronousSocketChannel result, Object attachment) {
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
        return !closed;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (acceptPending.getAndSet(false) && nativeSocket != null) {
                nativeSocket.onException(ex -> {}); 
            }
            if (nativeSocket != null) {
                nativeSocket.close();
            }
        }
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        if (!bound) throw new NotYetBoundException();
        if (nativeSocket == null) {
            return null;
        }
        return new InetSocketAddress(nativeSocket.ipAddress(), nativeSocket.port());
    }

    // Unsupported operations
    @Override
    public <T> AsynchronousServerSocketChannel setOption(SocketOption<T> name, T value) throws IOException {
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
            if (name == StandardSocketOptions.SO_RCVBUF) {
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("SO_RCVBUF expects Integer value");
                }
                Integer bufSize = (Integer) value;
                if (bufSize < 0) {
                    throw new IllegalArgumentException("Buffer size cannot be negative");
                }
                nativeSocket.setSocketOption(2, bufSize);
            } else if (name == StandardSocketOptions.SO_REUSEADDR) {
                if (!(value instanceof Boolean)) {
                    throw new IllegalArgumentException("SO_REUSEADDR expects Boolean value");
                }
                Boolean reuseAddr = (Boolean) value;
                nativeSocket.setSocketOption(4, reuseAddr ? 1 : 0);
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
            if (name == StandardSocketOptions.SO_RCVBUF) {
                return (T) Integer.valueOf(nativeSocket.getSocketOption(2));
            } else if (name == StandardSocketOptions.SO_REUSEADDR) {
                return (T) Boolean.valueOf(nativeSocket.getSocketOption(4) != 0);
            }
        } catch (Exception e) {
            throw new IOException("Failed to get socket option " + name, e);
        }
        
        return null;
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        Set<SocketOption<?>> options = new HashSet<>();
        options.add(StandardSocketOptions.SO_RCVBUF);
        options.add(StandardSocketOptions.SO_REUSEADDR);
        return Collections.unmodifiableSet(options);
    }
} 