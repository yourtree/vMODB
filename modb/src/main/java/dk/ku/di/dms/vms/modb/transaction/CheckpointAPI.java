package dk.ku.di.dms.vms.modb.transaction;

/**
 * Interface to which client classes (i.e., event handler) can request a checkpoint of the state
 */
public interface CheckpointAPI {

    void checkpoint();

}
