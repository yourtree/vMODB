package dk.ku.di.dms.vms.coordinator.server.coordinator;

/**
 * Different policies impact throughput of the system
 * and complexity entailed by the batch commit process
 */
public enum BatchEmissionPolicy {

    /**
     * emission of new batches are blocked until the current batch is committed or aborted.
     * this may allow for subsequent batch with huge amount of transactions and potentially higher chance if abortions
     */
    BLOCKING,
    OPTIMISTIC, // keep sending transactions does not matter whether the current batch has finished or not

}
