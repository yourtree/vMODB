package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.sdk.embed.handler.VmsEventHandler.DEFAULT_DELAY_FOR_BATCH_SEND;

/**
 * This class is responsible for all writes to the leader.
 * For now the methods are not inserting the same message again in the queue because
 * still not sure how leader is logging state after a crash
 * If so, may need to reinsert to continue the protocol from the same point
 */
final class LeaderWorker extends StoppableRunnable {

    private final Logger logger;

    private final ServerNode leader;

    private final LockConnectionMetadata leaderConnectionMetadata;

    private final BlockingDeque<TransactionEvent.Payload> eventsToSendToLeader;

    private final BlockingQueue<Message> leaderWorkerQueue;

    private final VmsNode vmsNode;

    /**
     * Messages that correspond to operations that can only be
     * spawned when a set of asynchronous messages arrive
     */
    enum Command {
        SEND_BATCH_COMPLETE, // inform batch completion
        SEND_BATCH_COMMIT_ACK, // inform commit completed
        SEND_TRANSACTION_ABORT // inform that a tid aborted
    }

    record Message(Command type, Object object){

        public BatchCommitAck.Payload asBatchCommitAck() {
            return (BatchCommitAck.Payload)object;
        }

        public BatchComplete.Payload asBatchComplete(){
            return (BatchComplete.Payload)object;
        }

        public TransactionAbort.Payload asTransactionAbort(){
            return (TransactionAbort.Payload)object;
        }

    }

    public LeaderWorker(VmsNode vmsNode,
                        ServerNode leader,
                        LockConnectionMetadata leaderConnectionMetadata,
                        BlockingDeque<TransactionEvent.Payload> eventsToSendToLeader,
                        BlockingQueue<Message> leaderWorkerQueue){
        this.vmsNode = vmsNode;
        this.leader = leader;
        this.leaderConnectionMetadata = leaderConnectionMetadata;
        this.eventsToSendToLeader = eventsToSendToLeader;
        this.leaderWorkerQueue = leaderWorkerQueue;
        this.logger = Logger.getLogger("leader-worker-"+leader.toString());
        this.logger.setUseParentHandlers(true);
    }

    @Override
    public void run() {

        // logger.info("Leader worker started!");

        while (isRunning()){
            try {

                this.batchEventsToLeader();

                // drain the queue
                while(true) {
                    Message msg = this.leaderWorkerQueue.poll(DEFAULT_DELAY_FOR_BATCH_SEND, TimeUnit.MILLISECONDS);
                    if (msg == null) break;
                    logger.info(vmsNode.identifier+": Leader worker will send message type: "+ msg.type());
                    switch (msg.type()) {
                        case SEND_BATCH_COMPLETE -> this.sendBatchComplete(msg.asBatchComplete());
                        case SEND_BATCH_COMMIT_ACK -> this.sendBatchCommitAck(msg.asBatchCommitAck());
                        case SEND_TRANSACTION_ABORT -> this.sendTransactionAbort(msg.asTransactionAbort());
                    }
                }

            } catch (Exception e) { // (InterruptedException e) {
                logger.warning(vmsNode.identifier+": Error on taking message from worker queue: "+e.getMessage());
            }
        }
    }

    private void write() {
        this.leaderConnectionMetadata.writeBuffer.flip();
        try {
            this.leaderConnectionMetadata.channel.write(this.leaderConnectionMetadata.writeBuffer).get();
        } catch (InterruptedException | ExecutionException e){
            this.leaderConnectionMetadata.writeBuffer.clear();
            if(!leaderConnectionMetadata.channel.isOpen()) {
                leader.off();
                this.stop();
            }
        } finally {
            this.leaderConnectionMetadata.writeBuffer.clear();
        }
    }

    private final List<TransactionEvent.Payload> events = new ArrayList<>();

    /**
     * No fault tolerance implemented. Once the events are submitted, they get lost and can
     * no longer be submitted to the leader.
     * In a later moment, to support crashes in the leader, we can create control messages
     * for acknowledging batch reception. This way, we could hold batches in memory until
     * the acknowledgment arrives
     */
    private void batchEventsToLeader() {

        this.eventsToSendToLeader.drainTo(this.events);

        int remaining = this.events.size();

        while(remaining > 0){

            logger.info(vmsNode.identifier+": Leader worker will send a batch of events to leader");

            remaining = BatchUtils.assembleBatchPayload( remaining, this.events, this.leaderConnectionMetadata.writeBuffer);
            try {
                this.leaderConnectionMetadata.writeBuffer.flip();
                this.leaderConnectionMetadata.channel.write(this.leaderConnectionMetadata.writeBuffer).get();
            } catch (InterruptedException | ExecutionException e) {

                // return events to the deque
                for(TransactionEvent.Payload event : this.events) {
                    this.eventsToSendToLeader.offerFirst(event);
                }

                if(!this.leaderConnectionMetadata.channel.isOpen()){
                    this.leader.off();
                    remaining = 0; // force exit loop
                }

            } finally {
                this.leaderConnectionMetadata.writeBuffer.clear();
            }
        }

    }

    private void sendBatchComplete(BatchComplete.Payload payload) {
        BatchComplete.write( this.leaderConnectionMetadata.writeBuffer, payload );
        write();
    }

    private void sendBatchCommitAck(BatchCommitAck.Payload payload) {
        BatchCommitAck.write( leaderConnectionMetadata.writeBuffer, payload );
        write();
    }

    private void sendTransactionAbort(TransactionAbort.Payload payload) {
        TransactionAbort.write( leaderConnectionMetadata.writeBuffer, payload );
        write();
    }

}