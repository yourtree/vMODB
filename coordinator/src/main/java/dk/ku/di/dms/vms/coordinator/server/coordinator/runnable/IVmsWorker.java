package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

/**
 * Interface that represents a unit of work
 * that encapsulates all operations and
 * messages exchanged between the coordinator
 * and the associated virtual microservice.
 * Interface useful for decoupling the test of
 * the batch protocol with the network protocol
 */
public interface IVmsWorker {

    void queueTransactionEvent(TransactionEvent.PayloadRaw payload);

    void queueMessage(Object message);

}
