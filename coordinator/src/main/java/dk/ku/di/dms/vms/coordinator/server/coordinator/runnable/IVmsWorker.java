package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;

/**
 * Interface that represents a unit of work
 * that encapsulates all operations and
 * messages exchanged between the coordinator
 * and the associated virtual microservice.
 * Interface useful for decoupling the test of
 * the batch protocol with the network protocol
 */
public interface IVmsWorker {

    /**
     * Messages that correspond to operations
     */
    record Message(Command type, Object object){

        public BatchCommitCommand.Payload asBatchCommitCommand() {
            return (BatchCommitCommand.Payload)object;
        }

        public String asVmsConsumerSet(){
            return (String)object;
        }

        public BatchCommitInfo.Payload asBatchOfEventsRequest(){
            return (BatchCommitInfo.Payload)object;
        }

        public TransactionAbort.Payload asTransactionAbort(){
            return (TransactionAbort.Payload)object;
        }

    }

    enum Command {
        SEND_BATCH_OF_EVENTS,
        SEND_BATCH_OF_EVENTS_WITH_COMMIT_INFO, // to terminals only
        SEND_BATCH_COMMIT_COMMAND,
        SEND_TRANSACTION_ABORT,
        SEND_CONSUMER_SET
    }

    enum State {
        NEW,
        CONNECTION_ESTABLISHED,
        CONNECTION_FAILED,
        LEADER_PRESENTATION_SENT,
        LEADER_PRESENTATION_SEND_FAILED,
        VMS_PRESENTATION_RECEIVED,
        VMS_PRESENTATION_RECEIVE_FAILED,
        VMS_PRESENTATION_PROCESSED,
        CONSUMER_SET_READY_FOR_SENDING,
        CONSUMER_SET_SENDING_FAILED,
        CONSUMER_EXECUTING
    }

    BlockingDeque<TransactionEvent.Payload> transactionEventsPerBatch(long batch);

    BlockingQueue<Message> queue();

}
