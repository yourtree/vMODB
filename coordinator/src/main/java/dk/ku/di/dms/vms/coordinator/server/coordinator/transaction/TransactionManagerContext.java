package dk.ku.di.dms.vms.coordinator.server.coordinator.transaction;

import dk.ku.di.dms.vms.coordinator.infra.SimpleQueue;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;

import java.util.List;
import java.util.Map;

public class TransactionManagerContext {

    // transaction manager actions
    // this provides a natural separation of tasks in the transaction manager thread. commit handling, transaction parsing, leaving the main thread free (only sending heartbeats)
    public SimpleQueue<byte> actionQueue;

    public Map<Integer, VmsIdentifier> VMSs;

    public Map<Integer, List<VmsIdentifier>> vmsConsumerSet;

    public Map<String, TransactionDAG> transactionMap;

    // must update the "me" on snapshotting (i.e., committing)
    public long tid;

    // the offset of the pending batch commit
    public long batchOffsetPendingCommit;

    // the current batch on which new transactions are being generated for
    public long batchOffset;

    // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
    public Map<Long, BatchContext> batchContextMap;

    // defines how the batch metadata is replicated across servers
    public BatchReplicationStrategy batchReplicationStrategy;

    // channels
    public SimpleQueue<BatchComplete.Payload> batchCompleteEvents;

    public SimpleQueue<TransactionAbort.Payload> transactionAbortEvents;

}