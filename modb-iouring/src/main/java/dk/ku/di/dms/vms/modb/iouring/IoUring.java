package dk.ku.di.dms.vms.modb.iouring;

import dk.ku.di.dms.vms.modb.iouring.util.NativeLibraryLoader;
import dk.ku.di.dms.vms.modb.iouring.util.ReferenceCounter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

/**
 * Primary interface for creating and working with an {@code io_uring}.
 */
public class IoUring {
    // Load the native library when this class is first loaded
    static {
        NativeLibraryLoader.load();
    }

    private static final int DEFAULT_MAX_EVENTS = 1024;
    private static final int EVENT_TYPE_ACCEPT = 0;
    private static final int EVENT_TYPE_READ = 1;
    private static final int EVENT_TYPE_WRITE = 2;
    private static final int EVENT_TYPE_CONNECT = 3;
    private static final int EVENT_TYPE_CLOSE = 4;
    private static final int EVENT_TYPE_FSYNC = 5;

    private final long ring;
    private final int ringSize;
    private final IntObjectHashMap<AbstractIoUringChannel> fdToSocket = new IntObjectHashMap<>();
    private Consumer<Exception> exceptionHandler;
    private boolean closed = false;
    private final long cqes;
    private final ByteBuffer resultBuffer;

    /**
     * Instantiates a new {@code IoUring} with {@code DEFAULT_MAX_EVENTS}.
     */
    public IoUring() {
        this(DEFAULT_MAX_EVENTS);
    }

    /**
     * Instantiates a new Io uring.
     *
     * @param ringSize the max events
     */
    public IoUring(int ringSize) {
        this.ringSize = ringSize;
        this.ring = IoUring.create(ringSize);
        this.cqes = IoUring.createCqes(ringSize);
        this.resultBuffer = ByteBuffer.allocateDirect(ringSize * 17);
    }

    /**
     * Closes the io_uring.
     */
    public void close() {
        synchronized (this) {
            if (closed) {
                return; // Already closed, just return instead of throwing exception
            }
            closed = true;
            IoUring.close(ring);
            IoUring.freeCqes(cqes);
        }
    }

    /**
     * Takes over the current thread with a loop calling {@code execute()}, until closed.
     */
    public void loop() {
        while (!closed && !Thread.currentThread().isInterrupted()) {
            try {
                // Use non-blocking execution to allow for interruption
                int count = executeNow();
                if (count == 0) {
                    // No events processed, sleep briefly to avoid busy waiting
                    Thread.sleep(1);
                }
            } catch (InterruptedException e) {
                // Thread was interrupted, break the loop
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // If we get an exception and we're closed or interrupted, break the loop
                if (closed || Thread.currentThread().isInterrupted()) {
                    break;
                }
                // Otherwise, let the exception handler deal with it
                if (exceptionHandler != null) {
                    exceptionHandler.accept(e);
                }
            }
        }
    }

    /**
     * Submits all queued I/O operations to the kernel and waits an unlimited amount of time for any to complete.
     */
    public int execute() {
        return doExecute(true);
    }

    /**
     * Submits all queued I/O operations to the kernel and handles any pending completion events, returning immediately
     * if none are present.
     */
    public int executeNow() {
        return doExecute(false);
    }

    private int doExecute(boolean shouldWait) {
        synchronized (this) {
            if (closed) {
                return 0; // Return 0 instead of throwing exception if closed
            }
            try {
                int count = IoUring.submitAndGetCqes(ring, resultBuffer, cqes, ringSize, shouldWait);
                if (count > 0) {
                    // System.err.printf("[IoUring.doExecute] Processing %d completion events%n", count);
                }
                for (int i = 0; i < count && i < ringSize; i++) {
                    boolean processedSuccessfully = false;
                    try {
                        // Double-check we're still not closed before processing
                        if (closed) {
                            break;
                        }
                        handleEventCompletion(cqes, resultBuffer, i);
                        processedSuccessfully = true;
                    } finally {
                        // Only mark as seen if we successfully processed AND we're still not closed
                        if (processedSuccessfully && !closed) {
                            IoUring.markCqeSeen(ring, cqes, i);
                        }
                    }
                }
                return count;
            } catch (Exception ex) {
                if (exceptionHandler != null && !closed) {
                    exceptionHandler.accept(ex);
                }
            } finally {
                if (!closed) {
                    resultBuffer.clear();
                }
            }
            return -1;
        }
    }

    private void handleEventCompletion(long cqes, ByteBuffer results, int i) {
        int result = results.getInt();
        int fd = results.getInt();
        int eventType = results.get();

        if (eventType == EVENT_TYPE_ACCEPT) {
            IoUringServerSocket serverSocket = (IoUringServerSocket) fdToSocket.get(fd);
            String ipAddress = IoUring.getCqeIpAddress(cqes, i);
            IoUringSocket socket = serverSocket.handleAcceptCompletion(this, serverSocket, result, ipAddress);
            if (socket != null) {
                fdToSocket.put(socket.fd(), socket);
            }
        } else {
            AbstractIoUringChannel channel = fdToSocket.get(fd);
            if (channel == null || channel.isClosed()) {
                return;
            }
            try {
                if (eventType == EVENT_TYPE_CONNECT) {
                    ((IoUringSocket) channel).handleConnectCompletion(this, result);
                } else if (eventType == EVENT_TYPE_READ) {
                    long bufferAddress = results.getLong();
                    ReferenceCounter<ByteBuffer> refCounter = channel.readBufferMap().get(bufferAddress);
                    if (refCounter == null) {
                        // Buffer was already removed, skip processing
                        return;
                    }
                    ByteBuffer buffer = refCounter.ref();
                    if (buffer == null) {
                        throw new IllegalStateException("Buffer already removed");
                    }
                    if (refCounter.deincrementReferenceCount() == 0) {
                        channel.readBufferMap().remove(bufferAddress);
                    }
                    channel.handleReadCompletion(buffer, result);
                } else if (eventType == EVENT_TYPE_WRITE) {
                    long bufferAddress = results.getLong();
                    ReferenceCounter<ByteBuffer> refCounter = channel.writeBufferMap().get(bufferAddress);
                    if (refCounter == null) {
                        // Buffer was already removed, skip processing
                        return;
                    }
                    ByteBuffer buffer = refCounter.ref();
                    if (buffer == null) {
                        throw new IllegalStateException("Buffer already removed");
                    }
                    if (refCounter.deincrementReferenceCount() == 0) {
                        channel.writeBufferMap().remove(bufferAddress);
                    }
                    channel.handleWriteCompletion(buffer, result);
                } else if (eventType == EVENT_TYPE_CLOSE) {
                    channel.setClosed(true);
                    channel.closeHandler().run();
                } else if (eventType == EVENT_TYPE_FSYNC) {
                    // Fsync completed, nothing special to do
                    // The result contains the return code (0 for success, negative for error)
                    if (result < 0) {
                        throw new IOException("Fsync failed with error code: " + result);
                    }
                }
            } catch (Exception ex) {
                if (channel.exceptionHandler() != null) {
                    channel.exceptionHandler().accept(ex);
                }
            } finally {
                if (channel.isClosed() && channel.equals(fdToSocket.get(fd))) {
                    deregister(channel);
                }
            }
        }
    }

    /**
     * Queues a {@link IoUringServerSocket} for an accept operation on the next ring execution.
     *
     * @param serverSocket the server socket
     * @return this instance
     */
    public IoUring queueAccept(IoUringServerSocket serverSocket) {
        fdToSocket.put(serverSocket.fd(), serverSocket);
        IoUring.queueAccept(ring, serverSocket.fd());
        return this;
    }

    /**
     * Queues a {@link IoUringServerSocket} for a connect operation on the next ring execution.
     *
     * @param socket the socket channel
     * @return this instance
     */
    public IoUring queueConnect(IoUringSocket socket) {
        fdToSocket.put(socket.fd(), socket);
        IoUring.queueConnect(ring, socket.fd(), socket.ipAddress(), socket.port());
        return this;
    }

    /**
     * Queues {@link IoUringSocket} for a read operation on the next ring execution.
     *
     * @param channel the channel
     * @param buffer the buffer to read into
     * @return this instance
     */
    public IoUring queueRead(AbstractIoUringChannel channel, ByteBuffer buffer) {
        return queueRead(channel, buffer, 0L);
    }

    /**
     * Queues {@link IoUringSocket} for a read operation on the next ring execution.
     *
     * @param channel the channel
     * @param buffer the buffer to read into
     * @param offset the offset into the file/source of the read; Casted to u64
     * @return this instance
     */
    public IoUring queueRead(AbstractIoUringChannel channel, ByteBuffer buffer, long offset) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be direct");
        }
        fdToSocket.put(channel.fd(), channel);
        long bufferAddress = IoUring.queueRead(ring, channel.fd(), buffer, buffer.position(), buffer.limit() - buffer.position(), offset);
        ReferenceCounter<ByteBuffer> refCounter = channel.readBufferMap().get(bufferAddress);
        if (refCounter == null) {
            refCounter = new ReferenceCounter<>(buffer);
            channel.readBufferMap().put(bufferAddress, refCounter);
        }
        refCounter.incrementReferenceCount();
        return this;
    }

    /**
     * Queues {@link IoUringSocket} for a write operation on the next ring execution.
     *
     * @param channel the channel
     * @return this instance
     */
    public IoUring queueWrite(AbstractIoUringChannel channel, ByteBuffer buffer) {
        return queueWrite(channel, buffer, 0L);
    }

    /**
     * Queues {@link IoUringSocket} for a write operation on the next ring execution.
     *
     * @param channel the channel
     * @param offset the offset into the file/source of the write; Casted to u64
     * @return this instance
     */
    public IoUring queueWrite(AbstractIoUringChannel channel, ByteBuffer buffer, long offset) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be direct");
        }
        fdToSocket.put(channel.fd(), channel);
        int bytesToWrite = buffer.limit() - buffer.position();
        long bufferAddress = IoUring.queueWrite(ring, channel.fd(), buffer, buffer.position(), bytesToWrite, offset);
        ReferenceCounter<ByteBuffer> refCounter = channel.writeBufferMap().get(bufferAddress);
        if (refCounter == null) {
            refCounter = new ReferenceCounter<>(buffer);
            channel.writeBufferMap().put(bufferAddress, refCounter);
        } else {
            System.err.printf("[IoUring] WARNING: Buffer already in map! fd=%d, bufferAddr=%x%n", 
                channel.fd(), bufferAddress);
        }
        refCounter.incrementReferenceCount();
        return this;
    }

    public IoUring queueClose(AbstractIoUringChannel channel) {
        IoUring.queueClose(ring, channel.fd());
        return this;
    }

    /**
     * Queues a file sync operation on the next ring execution.
     * 
     * @param channel the channel to sync
     * @param dataSyncOnly if true, only sync data (not metadata)
     * @return this instance
     */
    public IoUring queueFsync(AbstractIoUringChannel channel, boolean dataSyncOnly) {
        IoUring.queueFsync(ring, channel.fd(), dataSyncOnly);
        return this;
    }

    /**
     * Gets the exception handler.
     *
     * @return the exception handler
     */
    Consumer<Exception> exceptionHandler() {
        return exceptionHandler;
    }

    /**
     * Sets the handler that is called when an {@code Exception} is caught during execution.
     *
     * @param exceptionHandler the exception handler
     */
    public IoUring onException(Consumer<Exception> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    /**
     * Deregister this channel from the ring.
     *
     * @param channel the channel
     */
    void deregister(AbstractIoUringChannel channel) {
        fdToSocket.remove(channel.fd());
    }

    private static native long create(int maxEvents);
    private static native void close(long ring);
    private static native long createCqes(int count);
    private static native void freeCqes(long cqes);
    private static native int submitAndGetCqes(long ring, ByteBuffer buffer, long cqes, int cqesSize, boolean shouldWait);
    private static native String getCqeIpAddress(long cqes, int cqeIndex);
    private static native void markCqeSeen(long ring, long cqes, int cqeIndex);
    private static native void queueAccept(long ring, int serverSocketFd);
    private static native void queueConnect(long ring, int socketFd, String ipAddress, int port);
    private static native long queueRead(long ring, int channelFd, ByteBuffer buffer, int bufferPos, int bufferLen, long offset);
    private static native long queueWrite(long ring, int channelFd, ByteBuffer buffer, int bufferPos, int bufferLen, long offset);
    private static native void queueClose(long ring, int channelFd);
    private static native void queueFsync(long ring, int channelFd, boolean datasync);
}
