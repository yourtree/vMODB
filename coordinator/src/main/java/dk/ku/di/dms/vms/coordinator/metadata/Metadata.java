package dk.ku.di.dms.vms.coordinator.metadata;

import dk.ku.di.dms.vms.coordinator.VmsMetadata;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;

import java.util.Map;

/**
 * In-memory (off-heap) representation of the metadata maintained by the coordinator
 */
public class Metadata {

    public long committedOffset;

    // keyed by transaction name
    public Map<String, TransactionDAG> transactionMap;

    // keyed by vms name
    public Map<String, VmsMetadata> vmsMap;

}
