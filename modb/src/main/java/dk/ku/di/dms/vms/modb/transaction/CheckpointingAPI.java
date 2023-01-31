package dk.ku.di.dms.vms.modb.transaction;

/**
 * Interface to which clients can require a state checkpoint
 */
public interface CheckpointingAPI {

    void checkpoint();

}
