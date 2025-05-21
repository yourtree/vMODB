package dk.ku.di.dms.vms.web_common.meta;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

import dk.ku.di.dms.vms.web_common.channel.IChannel;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType;

/**
 * Reads are performed via single-thread anyway by design (completion handler),
 * but writes (and update to the channel after crashes) must be serialized to avoid concurrency errors
 * Some attributes are non-final to allow for dynamic reuse (e.g.,
 */
public final class LockConnectionMetadata extends ConnectionMetadata {

    public final ByteBuffer writeBuffer;

    public final ByteBuffer readBuffer;

    /*
     * Necessary to access connection metadata
     * The coordinator is responsible for keeping the connections up to date
     * The transaction manager just needs a read lock for the given connection
     */
    public final Semaphore writeLock;

    public LockConnectionMetadata(int key,
                                  NodeType nodeType,
                                  ByteBuffer readBuffer,
                                  ByteBuffer writeBuffer,
                                  IChannel channel,
                                  Semaphore writeLock) {
        super(key, nodeType, channel);
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
        this.writeLock = writeLock;
    }

}