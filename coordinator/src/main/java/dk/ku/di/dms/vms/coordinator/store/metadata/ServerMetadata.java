package dk.ku.di.dms.vms.coordinator.store.metadata;

import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.web_common.meta.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.meta.VmsIdentifier;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionEvent;

import java.util.List;
import java.util.Map;

/**
 * In-memory (off-heap) representation of the metadata
 * maintained by a server (coordinator and followers)
 */
public class ServerMetadata {

    public long committedOffset;

    // keyed by transaction name
    public Map<String, TransactionDAG> transactionMap;

    // keyed by vms name
    public Map<String, VmsIdentifier> vmsMap;

    public Map<String,Long> lastTidOfBatchPerVms;

    // keyed by vms name
    public Map<Integer, ServerIdentifier> serverMap;

    // key is the VMS identifier
    public Map<Integer, List<TransactionEvent.Payload>> transactionInputEventsInLastBatch;

}
