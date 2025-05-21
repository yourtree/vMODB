/**
 * IoUringUtils.java
 * 
 * Utility methods for the io_uring-based socket implementation.
 * Provides helper methods for working with direct ByteBuffers and common network operations.
 */
package dk.ku.di.dms.vms.sdk.embed.iouring;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Utility methods for the io_uring-based socket implementation.
 */
public final class IoUringUtils {
    
    private IoUringUtils() {
        // Prevent instantiation
    }
    
    /**
     * Creates a direct ByteBuffer with the specified capacity.
     * 
     * @param capacity the capacity of the buffer
     * @return a new direct ByteBuffer
     */
    public static ByteBuffer createDirectBuffer(int capacity) {
        NativeLibraryLoader.load();
        try {
            // Try native allocation first for better performance with io_uring
            ByteBuffer buffer = nativeAllocateDirectBuffer(capacity);
            if (buffer != null) {
                return buffer;
            }
        } catch (UnsatisfiedLinkError e) {
            // Fall back to JDK implementation
        }
        return ByteBuffer.allocateDirect(capacity);
    }
    
    /**
     * Gets the native address of a direct ByteBuffer.
     * 
     * @param buffer the direct buffer
     * @return the native address of the buffer, or 0 if the buffer is not direct
     */
    public static long getDirectBufferAddress(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return 0;
        }
        
        NativeLibraryLoader.load();
        try {
            return nativeGetDirectBufferAddress(buffer);
        } catch (UnsatisfiedLinkError e) {
            return 0;
        }
    }
    
    /**
     * Frees a direct ByteBuffer that was allocated with {@link #createDirectBuffer(int)}.
     * 
     * @param buffer the direct buffer to free
     */
    public static void freeDirectBuffer(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }
        
        NativeLibraryLoader.load();
        try {
            nativeFreeDirectBuffer(buffer);
        } catch (UnsatisfiedLinkError e) {
            // Ignore - JVM will clean up eventually
        }
    }
    
    /**
     * Checks if a buffer is a direct buffer and throws an exception if it is not.
     * 
     * @param buffer the buffer to check
     * @throws IllegalArgumentException if the buffer is not direct
     */
    public static void checkDirectBuffer(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be direct");
        }
    }
    
    /**
     * Configures a socket with optimal settings for high-performance networking.
     * 
     * @param channel the channel to configure
     * @param bufferSize the socket buffer size (used for both send and receive)
     * @throws IOException if an I/O error occurs
     */
    public static void configureSocket(IoUringChannel channel, int bufferSize) throws IOException {
        configureSocket(channel, bufferSize, bufferSize);
    }
    
    /**
     * Configures a socket with optimal settings for high-performance networking.
     * 
     * @param channel the channel to configure
     * @param soRcvBuf the socket receive buffer size
     * @param soSndBuf the socket send buffer size
     * @throws IOException if an I/O error occurs
     */
    public static void configureSocket(IoUringChannel channel, int soRcvBuf, int soSndBuf) throws IOException {
        channel.configureSocket(soRcvBuf, soSndBuf);
    }
    
    /**
     * Parses a string in the format "host:port" into an InetSocketAddress.
     * 
     * @param address the address string to parse
     * @return the parsed InetSocketAddress
     * @throws IllegalArgumentException if the address string is invalid
     */
    public static InetSocketAddress parseSocketAddress(String address) {
        String[] parts = address.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format: " + address);
        }
        
        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port number: " + parts[1]);
        }
        
        return new InetSocketAddress(host, port);
    }
    
    /**
     * Formats an InetSocketAddress into a string in the format "host:port".
     * 
     * @param address the address to format
     * @return the formatted address string
     */
    public static String formatSocketAddress(InetSocketAddress address) {
        return address.getHostString() + ":" + address.getPort();
    }
    
    /**
     * Gets the current version of liburing on the system.
     * 
     * @return the liburing version string or "not available" if liburing is not available
     */
    public static String getLibUringVersion() {
        try {
            NativeLibraryLoader.load();
            return nativeGetLibUringVersion();
        } catch (UnsatisfiedLinkError e) {
            return "not available";
        }
    }
    
    /**
     * Checks if io_uring is supported on the current system.
     * 
     * @return true if io_uring is supported, false otherwise
     */
    public static boolean isIoUringSupported() {
        try {
            // Try to load the library but don't fail if not available
            try {
                NativeLibraryLoader.load();
            } catch (UnsatisfiedLinkError e) {
                return false;
            }
            return nativeIsIoUringSupported();
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }
    
    // Native method declarations
    
    private static native String nativeGetLibUringVersion();
    private static native boolean nativeIsIoUringSupported();
    private static native ByteBuffer nativeAllocateDirectBuffer(int capacity);
    private static native void nativeFreeDirectBuffer(ByteBuffer buffer);
    private static native long nativeGetDirectBufferAddress(ByteBuffer buffer);
    private static native int nativeGetDirectBufferCapacity(ByteBuffer buffer);
} 