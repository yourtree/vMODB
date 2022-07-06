package dk.ku.di.dms.vms.coordinator.server.infra;

import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionEvent;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.VMS;

public final class VmsConnectionMetadata extends ConnectionMetadata {

    // in case the VMS fails, we can resend (the inputs only)
    // the internal events must be sent by the other VMSs in case this VMS is in the center of a DAG
    public Map<Long, List<TransactionEvent.Payload>> transactionEventsPerBatch;

    public VmsConnectionMetadata(int key, ByteBuffer readBuffer, ByteBuffer writeBuffer, AsynchronousSocketChannel channel, ReentrantLock writeLock) {
        super(key, VMS, readBuffer, writeBuffer, channel, writeLock);
        this.transactionEventsPerBatch = new HashMap<>();
    }

}
