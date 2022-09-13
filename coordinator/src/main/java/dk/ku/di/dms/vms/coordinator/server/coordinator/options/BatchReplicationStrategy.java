package dk.ku.di.dms.vms.coordinator.server.coordinator.options;

/**
 * Why do I need to replicate.
 * What happens if a leader fails?
 * I should be able to restart from the last committed batch.
 * We could also assume the leader can restart... I am trying to achieve high availability here
 */
public enum BatchReplicationStrategy {

    // one safe, just the leader.

    // two safe, it requires at least one replica to acknowledge reception of message
    AT_LEAST_ONE, // like kafka, at least one replica acknowledge and asynchronously I continue replicating to others. it frees the serves as soon as possbile

    MAJORITY, //
    ALL // all replicas acknowledge, sending in parallel

}
