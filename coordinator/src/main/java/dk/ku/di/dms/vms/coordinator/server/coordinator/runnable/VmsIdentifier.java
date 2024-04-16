package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;

/**
 * Class enriched with data structures to
 */
public record VmsIdentifier (VmsNode node, IVmsWorker worker) {

    public long getLastTidOfBatch(){
        return this.node.lastTidOfBatch;
    }

    public long getPreviousBatch(){
        return this.node.previousBatch;
    }

    public String getIdentifier(){
        return this.node.identifier;
    }

}

