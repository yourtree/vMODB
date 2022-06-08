package dk.ku.di.dms.vms.coordinator.server.schema.infra;

import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reads are performed via single-thread anyway by design (completion handler),
 * but writes (and update to the channel after crashes) must be serialized to avoid concurrency errors
 *
 * Some attributes are non-final
 */
public class ConnectionMetadata {
    public final ByteBuffer readBuffer;
    public final ByteBuffer writeBuffer;
    public AsynchronousSocketChannel channel;
    public final ReentrantLock writeLock;

    public EventIdentifier lastEventWritten;

    public ConnectionMetadata(ByteBuffer readBuffer, ByteBuffer writeBuffer, AsynchronousSocketChannel channel, ReentrantLock writeLock) {
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
        this.channel = channel;
        this.writeLock = writeLock;
    }
}