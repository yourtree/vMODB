/**
 * IoUringAsyncChannel.java
 * 
 * A wrapper for IoUringChannel that implements the IChannel interface.
 * This class provides compatibility with existing code that expects IChannel instances.
 */
package dk.ku.di.dms.vms.sdk.embed.iouring;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import dk.ku.di.dms.vms.web_common.channel.IChannel;

/**
 * A wrapper for IoUringChannel that implements the IChannel interface.
 * This allows the io_uring implementation to be used with existing code
 * that expects IChannel instances.
 */
public final class IoUringAsyncChannel implements IChannel {

    private final IoUringChannel channel;

    /**
     * Constructor.
     * 
     * @param channel the io_uring channel to wrap
     */
    public IoUringAsyncChannel(IoUringChannel channel) {
        this.channel = channel;
    }

    /**
     * Creates a new io_uring channel.
     * 
     * @param group the io_uring channel group
     * @return a new io_uring channel wrapper
     */
    public static IoUringAsyncChannel create(IoUringChannelGroup group) {
        try {
            return new IoUringAsyncChannel(IoUringChannel.open(group));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        this.channel.write(src, null, new CompletionHandler<Integer, Void>() {
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

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        this.channel.write(src, attachment, handler);
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        this.channel.read(dst, attachment, handler);
    }

    @Override
    public Future<Void> connect(InetSocketAddress inetSocketAddress) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.channel.connect(inetSocketAddress, null, new CompletionHandler<Void, Void>() {
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

    @Override
    public void close() {
        try {
            this.channel.closeQuietly();
        } catch (Exception ignored) {
            // Should not happen since closeQuietly() handles exceptions
        }
    }
    
    /**
     * Gets the underlying io_uring channel.
     * 
     * @return the underlying io_uring channel
     */
    public IoUringChannel getChannel() {
        return channel;
    }
} 