package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.System.Logger.Level.*;

/**
 * This class is responsible for all writes to the leader.
 * For now the methods are not inserting the same message again in the queue because
 * still not sure how leader is logging state after a crash
 * If so, may need to reinsert to continue the protocol from the same point
 */
final class LeaderWorker extends StoppableRunnable {

    private static final System.Logger logger = System.getLogger(LeaderWorker.class.getName());

    private final ServerNode leader;

    private final LockConnectionMetadata leaderConnectionMetadata;

    // private final BlockingDeque<TransactionEvent.PayloadRaw> eventsToSendToLeader;

    private final BlockingQueue<Message> leaderWorkerQueue;

    private final VmsNode vmsNode;

    /**
     * Messages that correspond to operations that can only be
     * spawned when a set of asynchronous messages arrive
     */
    enum Command {
        SEND_EVENT,
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

        public TransactionEvent.PayloadRaw asEvent(){
            return (TransactionEvent.PayloadRaw)object;
        }

    }

    public LeaderWorker(VmsNode vmsNode,
                        ServerNode leader,
                        LockConnectionMetadata leaderConnectionMetadata,
                        BlockingQueue<Message> leaderWorkerQueue){
        this.vmsNode = vmsNode;
        this.leader = leader;
        this.leaderConnectionMetadata = leaderConnectionMetadata;
        this.leaderWorkerQueue = leaderWorkerQueue;
    }

    private static final boolean BLOCKING = true;

    private static final int MAX_TIMEOUT = 5000;

    @Override
    public void run() {
        logger.log(INFO, vmsNode.identifier+": Leader worker started!");
        int pollTimeout = 50;
        Message msg;
        while (this.isRunning()){
            try {
                if(BLOCKING){
                    msg = this.leaderWorkerQueue.take();
                } else {
                    msg = this.leaderWorkerQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
                    if (msg == null) {
                        pollTimeout = Math.min(pollTimeout * 2, MAX_TIMEOUT);
                        continue;
                    }
                    pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
                }

                logger.log(DEBUG, vmsNode.identifier+": Leader worker will send message type: "+ msg.type());
                switch (msg.type()) {
                    case SEND_BATCH_COMPLETE -> this.sendBatchComplete(msg.asBatchComplete());
                    case SEND_BATCH_COMMIT_ACK -> this.sendBatchCommitAck(msg.asBatchCommitAck());
                    case SEND_TRANSACTION_ABORT -> this.sendTransactionAbort(msg.asTransactionAbort());
                    case SEND_EVENT -> this.sendEvent(msg.asEvent());
                }
            } catch (Exception e) {
                logger.log(WARNING, vmsNode.identifier+": Error on taking message from worker queue: "+e.getCause().getMessage());
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

    /**
     * No fault tolerance implemented. Once the events are submitted, they get lost and can
     * no longer be submitted to the leader.
     * In a later moment, to support crashes in the leader, we can create control messages
     * for acknowledging batch reception. This way, we could hold batches in memory until
     * the acknowledgment arrives
     */
    private void sendEvent(TransactionEvent.PayloadRaw payloadRaw) {
        TransactionEvent.write( this.leaderConnectionMetadata.writeBuffer, payloadRaw );
        write();
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