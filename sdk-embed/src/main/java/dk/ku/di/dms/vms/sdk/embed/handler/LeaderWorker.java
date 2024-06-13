package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import static java.lang.System.Logger.Level.*;
import static java.lang.Thread.sleep;

/**
 * This class is responsible for all writes to the leader.
 * For now the methods are not inserting the same message again in the queue because
 * still not sure how leader is logging state after a crash
 * If so, may need to reinsert to continue the protocol from the same point
 */
final class LeaderWorker extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(LeaderWorker.class.getName());

    private final ServerNode leader;

    private final AsynchronousSocketChannel channel;
    
    private final ByteBuffer writeBuffer;

    private final Queue<Object> leaderWorkerQueue;

    private final VmsNode vmsNode;

    public LeaderWorker(VmsNode vmsNode,
                        ServerNode leader,
                        AsynchronousSocketChannel channel,
                        ByteBuffer writeBuffer){
        this.vmsNode = vmsNode;
        this.leader = leader;
        this.channel = channel;
        this.writeBuffer = writeBuffer;
        this.leaderWorkerQueue = new ConcurrentLinkedQueue<>();
    }

    private static final int MAX_TIMEOUT = 500;

    @Override
    @SuppressWarnings("BusyWait")
    public void run() {
        LOGGER.log(INFO, this.vmsNode.identifier+": Leader worker started!");
        int pollTimeout = 1;
        Object message = null;
        while (this.isRunning()){
            try {
                message = this.leaderWorkerQueue.poll();
                if (message == null) {
                    pollTimeout = Math.min(pollTimeout * 2, MAX_TIMEOUT);
                    sleep(pollTimeout);
                    continue;
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;

                LOGGER.log(DEBUG, this.vmsNode.identifier+": Leader worker will send message type: "+ message.getClass().getName());

                this.sendMessage(message);

            } catch (Exception e) {
                LOGGER.log(ERROR, this.vmsNode.identifier+": Error on taking message from worker queue: "+e.getCause().getMessage());
                if(message != null){
                    this.queueMessage(message);
                }
            }
        }
    }

    private void sendMessage(Object message) {
        switch (message) {
            case BatchComplete.Payload o -> this.sendBatchComplete(o);
            case BatchCommitAck.Payload o -> this.sendBatchCommitAck(o);
            case TransactionAbort.Payload o -> this.sendTransactionAbort(o);
            case TransactionEvent.PayloadRaw o -> this.sendEvent(o);
            default -> LOGGER.log(WARNING, this.vmsNode.identifier +
                    ": Leader worker do not recognize message type: " + message.getClass().getName());
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void queueMessage(Object message) {
        while(!this.promise.isDone());
        // only after done one can clear the buffer
        this.writeBuffer.clear();
        this.sendMessage(message);
    }

    Future<Integer> promise = CompletableFuture.completedFuture(0);

    private void write(Object message) {
        try {
            this.writeBuffer.flip();
            this.promise = this.channel.write(this.writeBuffer);
            // this.writeBuffer.clear();
        } catch (Exception e){
            // queue to try insert again
            LOGGER.log(ERROR, this.vmsNode.identifier+": Error on writing message to Leader\n"+e.getCause().getMessage(), e);
            e.printStackTrace(System.out);
            this.queueMessage(message);
            this.writeBuffer.clear();
            if(!this.channel.isOpen()) {
                this.leader.off();
                this.stop();
            }
        }
    }

    /**
     * No fault tolerance implemented. Once the events are submitted, they get lost and can
     * no longer be submitted to the leader.
     * In a later moment, to support crashes in the leader, we can create control messages
     * for acknowledging batch reception. This way, we could hold batches in memory until
     * the acknowledgment arrives
     */
    private void sendEvent(TransactionEvent.PayloadRaw payload) {
        TransactionEvent.write( this.writeBuffer, payload );
        this.write(payload);
    }

    private void sendBatchComplete(BatchComplete.Payload payload) {
        BatchComplete.write( this.writeBuffer, payload );
        this.write(payload);
    }

    private void sendBatchCommitAck(BatchCommitAck.Payload payload) {
        BatchCommitAck.write( writeBuffer, payload );
        this.write(payload);
    }

    private void sendTransactionAbort(TransactionAbort.Payload payload) {
        TransactionAbort.write( writeBuffer, payload );
        this.write(payload);
    }

}