package dk.ku.di.dms.vms.coordinator.server.infra;

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

    // generic, serves for both servers and VMSs
    public int key;
    public final NodeType nodeType;

    public enum NodeType {
        SERVER,
        VMS
    }

    public final ByteBuffer readBuffer;
    public final ByteBuffer writeBuffer;
    public AsynchronousSocketChannel channel;

    // unique read thread by design (completion handler)
    public final ReentrantLock writeLock;

    public ConnectionMetadata(int key, NodeType nodeType, ByteBuffer readBuffer, ByteBuffer writeBuffer, AsynchronousSocketChannel channel, ReentrantLock writeLock) {
        this.key = key;
        this.nodeType = nodeType;
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
        this.channel = channel;
        this.writeLock = writeLock;
    }
}