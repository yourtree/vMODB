package dk.ku.di.dms.vms.coordinator.server.infra;

import dk.ku.di.dms.vms.coordinator.server.schema.transaction.TransactionEvent;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static dk.ku.di.dms.vms.coordinator.server.infra.ConnectionMetadata.NodeType.VMS;

public final class VmsConnectionMetadata extends ConnectionMetadata {

    public Map<Long, List<TransactionEvent.TransactionEventPayload>> transactionEventsPerBatch;

    public VmsConnectionMetadata(int key, ByteBuffer readBuffer, ByteBuffer writeBuffer, AsynchronousSocketChannel channel, ReentrantLock writeLock) {
        super(key, VMS, readBuffer, writeBuffer, channel, writeLock);
        this.transactionEventsPerBatch = new HashMap<>();
    }

}
