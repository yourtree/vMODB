package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

/**
 * Interface that represents a unit of work
 * that encapsulates all operations and
 * messages exchanged between the coordinator
 * and the associated virtual microservice.
 * Interface useful for decoupling the test of
 * the batch protocol with the network protocol
 */
public interface IVmsWorker {

    void queueMessage(Object message);

}
