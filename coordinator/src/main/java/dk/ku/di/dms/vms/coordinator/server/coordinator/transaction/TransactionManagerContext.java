package dk.ku.di.dms.vms.coordinator.server.coordinator.transaction;

import dk.ku.di.dms.vms.coordinator.infra.SimpleQueue;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.web_common.meta.VmsIdentifier;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchComplete;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionAbort;

import java.util.List;
import java.util.Map;

public record TransactionManagerContext (

        // transaction manager actions
        // this provides a natural separation of tasks in the transaction manager thread. commit handling, transaction parsing, leaving the main thread free (only sending heartbeats)
        SimpleQueue<byte> actionQueue,

        Map<Integer, VmsIdentifier>VMSs,

        Map<Integer, List<VmsIdentifier>> vmsConsumerSet,

        Map<String, TransactionDAG> transactionMap,

        // must update the "me" on snapshotting (i.e., committing)
       long tid,

        // the offset of the pending batch commit
        long batchOffsetPendingCommit,

        // the current batch on which new transactions are being generated for
        long batchOffset,

        // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
        Map<Long, BatchContext> batchContextMap,

        // defines how the batch metadata is replicated across servers
        BatchReplicationStrategy batchReplicationStrategy,

        // channels
        SimpleQueue<BatchComplete.Payload> batchCompleteEvents,
        SimpleQueue<TransactionAbort.Payload> transactionAbortEvents
){}