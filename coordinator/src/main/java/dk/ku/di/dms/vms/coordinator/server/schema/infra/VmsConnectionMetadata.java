package dk.ku.di.dms.vms.coordinator.server.schema.infra;

import dk.ku.di.dms.vms.coordinator.metadata.VmsIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.locks.ReentrantLock;

public class VmsConnectionMetadata extends ConnectionMetadata {

    public EventIdentifier lastEventWritten;
    public volatile long ongoingBatch;

    public VmsConnectionMetadata(int key, ByteBuffer readBuffer, ByteBuffer writeBuffer, AsynchronousSocketChannel channel, ReentrantLock writeLock) {
        super(key, readBuffer, writeBuffer, channel, writeLock);
    }

}
