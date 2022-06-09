package dk.ku.di.dms.vms.coordinator.server.schema.infra;

import dk.ku.di.dms.vms.coordinator.metadata.VmsIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.locks.ReentrantLock;

public class VmsConnectionMetadata extends ConnectionMetadata {

    public final VmsIdentifier vms;
    public EventIdentifier lastEventWritten;

    public VmsConnectionMetadata(VmsIdentifier vms, ByteBuffer readBuffer, ByteBuffer writeBuffer, AsynchronousSocketChannel channel, ReentrantLock writeLock) {
        super(readBuffer, writeBuffer, channel, writeLock);
        this.vms = vms;
    }

}
