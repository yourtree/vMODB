package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.*;

import static java.lang.System.Logger.Level.*;

/**
 * This thread encapsulates the batch of events sending task
 * that should occur periodically. Once set up, it schedules itself
 * after each run, thus avoiding duplicate runs of the same task.
 * -
 * I could get the connection from the vms...
 * But in the future, one output event will no longer map to a single vms
 * So it is better to make the event sender task complete enough
 * what happens if the connection fails? then put the list of events
 *  in the batch to resend or return to the original location. let the
 *  main loop schedule the timer again. set the network node to off
 */
@SuppressWarnings("SequencedCollectionMethodCanBeUsed")
final class ConsumerVmsWorker extends StoppableRunnable {

    private static final System.Logger logger = System.getLogger(ConsumerVmsWorker.class.getName());
    
    private final VmsNode me;
    
    private final ConsumerVms consumerVms;
    private final ConnectionMetadata connectionMetadata;

    private final Deque<ByteBuffer> writeBufferPool;

    private final BlockingQueue<Byte> WRITE_SYNCHRONIZER = new ArrayBlockingQueue<>(1);

    private static final Byte DUMB = 1;

    private final int networkBufferSize;

    public ConsumerVmsWorker(VmsNode me, ConsumerVms consumerVms, ConnectionMetadata connectionMetadata, int networkBufferSize){
        this.me = me;
        this.consumerVms = consumerVms;
        this.connectionMetadata = connectionMetadata;
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(networkBufferSize) );
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(networkBufferSize) );

        // to allow the first thread to write
        this.WRITE_SYNCHRONIZER.add(DUMB);

        this.networkBufferSize = networkBufferSize;
    }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private static final int MAX_TIMEOUT = 1000;

    private static final boolean BLOCKING = true;

    @Override
    public void run() {

        logger.log(INFO, this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);

        int pollTimeout = 1;
        TransactionEvent.PayloadRaw payloadRaw;
        List<TransactionEvent.PayloadRaw> events = new ArrayList<>(1024);
        while(this.isRunning()){
            try {
                if(BLOCKING){
                    payloadRaw = this.consumerVms.transactionEvents.take();
                } else {
                    payloadRaw = this.consumerVms.transactionEvents.poll(pollTimeout, TimeUnit.MILLISECONDS);
                    if (payloadRaw == null) {
                        pollTimeout = Math.min(pollTimeout * 2, MAX_TIMEOUT);
                        continue;
                    }
                    pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;

                }
            } catch (InterruptedException ignored) {
                continue;
            }
            events.add(payloadRaw);
            this.consumerVms.transactionEvents.drainTo(events);

            if(!events.isEmpty()){
                this.sendBatchOfEvents(events);
            }

            this.processPendingWrites();
        }
    }

    private void processPendingWrites() {
        // do we have pending writes
        ByteBuffer bb = PENDING_WRITES_BUFFER.poll();
        if (bb != null) {
            try {
                logger.log(INFO, "Leader: Retrying sending failed buffer send");
                this.WRITE_SYNCHRONIZER.take();
                this.connectionMetadata.channel.write(bb, 1000L, TimeUnit.MILLISECONDS, bb, this.writeCompletionHandler);
            } catch (Exception ignored) { }
        }
    }

    private void sendBatchOfEvents(List<TransactionEvent.PayloadRaw> events) {
        int remaining = events.size();
        int count = remaining;
        ByteBuffer writeBuffer;
        while(remaining > 0){
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, events, writeBuffer);

                logger.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;

                // the flip will allow the underlying network stack to break the message into two or more deliveries
                // that would be nice if the expectation of the receiver were not receiving the whole message at once
                // without the flip, given the send and recv buffer are both the size of the message,
                // that guarantees the entire buffer will be delivered at once
                // writeBuffer.flip();
                writeBuffer.position(0);

                this.WRITE_SYNCHRONIZER.take();
                this.connectionMetadata.channel.write(writeBuffer, 1000L, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);

                // prevents two batches from being placed in the same delivery
                // without sleep, two batches may be placed in the same buffer in the receiver
                // sleep(100)

            } catch (Exception e) {
                logger.log(ERROR, this.me.identifier+ ": Error submitting events to "+this.consumerVms.identifier);
                // return non-processed events to original location or what?
                if (!this.connectionMetadata.channel.isOpen()) {
                    logger.log(WARNING, "The "+this.consumerVms.identifier+" VMS is offline");
                }
                // return events to the deque
                while(!events.isEmpty()) {
                    if(this.consumerVms.transactionEvents.offerFirst(events.get(0))){
                        events.remove(0);
                    }
                }
            }
        }
        events.clear();
    }

    private ByteBuffer retrieveByteBuffer(){
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(this.networkBufferSize);
    }

    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        this.writeBufferPool.add(bb);
    }

    private final BlockingDeque<ByteBuffer> PENDING_WRITES_BUFFER = new LinkedBlockingDeque<>();

    private final class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {

        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            if(byteBuffer.hasRemaining()){
                logger.log(ERROR, me.identifier + " on completed. Consumer worker for "+consumerVms.identifier+" found not all bytes were sent!");
            } else {
                logger.log(DEBUG, me.identifier + ": Batch with size " + result + " has been sent to: " + consumerVms.identifier);
                WRITE_SYNCHRONIZER.add(DUMB);
                returnByteBuffer(byteBuffer);
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            logger.log(ERROR, me.identifier+": ERROR on writing batch of events to: "+consumerVms.identifier+": "+exc);
            PENDING_WRITES_BUFFER.add(byteBuffer);
        }

    }
}