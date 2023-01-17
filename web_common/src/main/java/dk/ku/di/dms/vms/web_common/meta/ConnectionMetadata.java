package dk.ku.di.dms.vms.web_common.meta;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.Semaphore;

/**
 * Reads are performed via single-thread anyway by design (completion handler),
 * but writes (and update to the channel after crashes) must be serialized to avoid concurrency errors
 * Some attributes are non-final to allow for dynamic reuse (e.g.,
 */
public class ConnectionMetadata {

    /**
     * generic, serves for both servers and VMSs, although the key
     * may change across different classes (e.g., use of vms name or <host+port>)
     */
    public int key;
    public final NodeType nodeType;

    public enum NodeType {
        SERVER,
        VMS,
        CLIENT
    }

    public ByteBuffer readBuffer;
    public final ByteBuffer writeBuffer;

    public AsynchronousSocketChannel channel;

    /*
     * Necessary to access connection metadata
     * The coordinator is responsible for keeping the connections up to date
     * The transaction manager just needs a read lock for the given connection
     */
    public final Semaphore writeLock;

    public ConnectionMetadata(int key,
                              NodeType nodeType,
                              ByteBuffer readBuffer,
                              ByteBuffer writeBuffer,
                              AsynchronousSocketChannel channel,
                              Semaphore writeLock) {
        this.key = key;
        this.nodeType = nodeType;
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
        this.channel = channel;
        this.writeLock = writeLock;
    }

}