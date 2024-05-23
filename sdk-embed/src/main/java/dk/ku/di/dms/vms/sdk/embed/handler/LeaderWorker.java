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
import java.nio.channels.CompletionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static java.lang.System.Logger.Level.*;

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

    // private final BlockingDeque<TransactionEvent.PayloadRaw> eventsToSendToLeader;

    private final BlockingQueue<Object> leaderWorkerQueue;

    private final VmsNode vmsNode;

    public LeaderWorker(VmsNode vmsNode,
                        ServerNode leader,
                        AsynchronousSocketChannel channel,
                        ByteBuffer writeBuffer){
        this.vmsNode = vmsNode;
        this.leader = leader;
        this.channel = channel;
        this.writeBuffer = writeBuffer;
        this.leaderWorkerQueue = new LinkedBlockingDeque<>();
    }

    private static final boolean BLOCKING = true;

    private static final int MAX_TIMEOUT = 5000;

    @Override
    public void run() {
        LOGGER.log(INFO, vmsNode.identifier+": Leader worker started!");
        int pollTimeout = 50;
        Object msg;
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

                LOGGER.log(DEBUG, vmsNode.identifier+": Leader worker will send message type: "+ msg.getClass().getName());
                try {
                    switch (msg) {
                        case BatchComplete.Payload o -> this.sendBatchComplete(o);
                        case BatchCommitAck.Payload o -> this.sendBatchCommitAck(o);
                        case TransactionAbort.Payload o -> this.sendTransactionAbort(o);
                        case TransactionEvent.PayloadRaw o -> this.sendEvent(o);
                        default ->
                                LOGGER.log(WARNING, vmsNode.identifier + ": Leader worker do not recognize message type: " + msg.getClass().getName());
                    }
                } catch (Exception e){
                    LOGGER.log(ERROR, vmsNode.identifier+": Error on processing message. \n Payload: \n " +e + "\n Error message \n"+ e.getMessage());
                    this.queueMessage(msg);
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, vmsNode.identifier+": Error on taking message from worker queue: "+e.getCause().getMessage());
            }
        }
    }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private final class WriteCompletionHandler implements CompletionHandler<Integer, Integer> {
        @Override
        public void completed(Integer result, Integer remaining) {
            int newRemaining = remaining - result;
            if(newRemaining > 0) {
                LOGGER.log(DEBUG, vmsNode.identifier+": Leader worker will send remaining bytes: "+newRemaining);
                channel.write(writeBuffer, newRemaining, this);
            } else {
                writeBuffer.clear();
            }
        }

        @Override
        public void failed(Throwable exc, Integer remaining) {
            LOGGER.log(ERROR, vmsNode.identifier+": Leader worker found an error: \n"+exc);
            writeBuffer.clear();
        }
    }

    private void write() {
        this.writeBuffer.flip();
        int remaining = this.writeBuffer.limit();
        try {
            this.channel.write(this.writeBuffer, remaining, this.writeCompletionHandler);
        } catch (Exception e){
            this.writeBuffer.clear();
            if(!channel.isOpen()) {
                leader.off();
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
    private void sendEvent(TransactionEvent.PayloadRaw payloadRaw) {
        TransactionEvent.write( this.writeBuffer, payloadRaw );
        write();
    }

    private void sendBatchComplete(BatchComplete.Payload payload) {
        BatchComplete.write( this.writeBuffer, payload );
        write();
    }

    private void sendBatchCommitAck(BatchCommitAck.Payload payload) {
        BatchCommitAck.write( writeBuffer, payload );
        write();
    }

    private void sendTransactionAbort(TransactionAbort.Payload payload) {
        TransactionAbort.write( writeBuffer, payload );
        write();
    }

    public void queueMessage(Object message) {
        boolean sent = this.leaderWorkerQueue.offer(message);
        while(!sent) {
            sent = this.leaderWorkerQueue.offer(message);
        }
    }

}