package dk.ku.di.dms.vms.coordinator.server.infra;

import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.locks.ReentrantLock;

import static dk.ku.di.dms.vms.coordinator.server.infra.ConnectionMetadata.NodeType.VMS;

public class VmsConnectionMetadata extends ConnectionMetadata {

    public EventIdentifier lastEventWritten;

    public VmsConnectionMetadata(int key, ByteBuffer readBuffer, ByteBuffer writeBuffer, AsynchronousSocketChannel channel, ReentrantLock writeLock) {
        super(key, VMS, readBuffer, writeBuffer, channel, writeLock);
    }

}
