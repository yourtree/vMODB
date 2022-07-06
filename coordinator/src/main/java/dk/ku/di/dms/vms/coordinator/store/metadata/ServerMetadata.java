package dk.ku.di.dms.vms.coordinator.store.metadata;

import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.web_common.meta.VmsIdentifier;

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

}
