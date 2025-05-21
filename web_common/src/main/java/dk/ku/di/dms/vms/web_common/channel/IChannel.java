package dk.ku.di.dms.vms.web_common.channel;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface IChannel {

    default Future<Integer> write(ByteBuffer src) { return CompletableFuture.completedFuture(0); }

    default <A> void write(ByteBuffer src,
                           long timeout,
                           TimeUnit unit,
                           A attachment,
                           CompletionHandler<Integer,? super A> handler) {}
    
    // Additional overloaded write methods for compatibility
    default <A> void write(ByteBuffer src, 
                           A attachment, 
                           CompletionHandler<Integer,? super A> handler) {
        write(src, 0, TimeUnit.MILLISECONDS, attachment, handler);
    }
    
    default void write(ByteBuffer src, 
                       CompletionHandler<Integer, ByteBuffer> handler) {
        write(src, src, handler);
    }
    
    default void write(ByteBuffer src, 
                       Object attachment, 
                       CompletionHandler<Integer, ?> handler) {
        write(src, 0, TimeUnit.MILLISECONDS, attachment, (CompletionHandler<Integer, Object>) handler);
    }

    default boolean isOpen() {
        return true;
    }

    default <A> void read(ByteBuffer dst,
                          A attachment,
                          CompletionHandler<Integer,? super A> handler) { }

    default Future<Void> connect(InetSocketAddress inetSocketAddress) { return CompletableFuture.completedFuture(null); }

    /**
     * Closes this channel without throwing exceptions.
     * Implementations that extend NetworkChannel should provide
     * a closeQuietly() method that calls close() in a try-catch block.
     */
    default void close() { }

}
