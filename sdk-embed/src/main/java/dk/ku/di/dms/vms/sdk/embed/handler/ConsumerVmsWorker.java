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
import static java.lang.Thread.sleep;

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

    private final int networkSendTimeout;

    private final BlockingQueue<ByteBuffer> PENDING_WRITES_BUFFER = new LinkedBlockingQueue<>();

    public ConsumerVmsWorker(VmsNode me, ConsumerVms consumerVms, ConnectionMetadata connectionMetadata, int networkBufferSize, int networkSendTimeout){
        this.me = me;
        this.consumerVms = consumerVms;
        this.connectionMetadata = connectionMetadata;
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(networkBufferSize) );
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(networkBufferSize) );

        // to allow the first thread to write
        this.WRITE_SYNCHRONIZER.add(DUMB);

        this.networkBufferSize = networkBufferSize;
        this.networkSendTimeout = networkSendTimeout;
    }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private static final int MAX_TIMEOUT = 1000;

    private static final boolean BLOCKING = true;

    @Override
    public void run() {

        logger.log(INFO, this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);

        List<TransactionEvent.PayloadRaw> events = new ArrayList<>(1024);
        int pollTimeout = 1;
        TransactionEvent.PayloadRaw payloadRaw;
        while(this.isRunning()){
            try {
                if(BLOCKING){
                    payloadRaw = this.consumerVms.transactionEvents.take();
                } else {
                    payloadRaw = this.consumerVms.transactionEvents.poll(pollTimeout, TimeUnit.MILLISECONDS);
                    if (payloadRaw == null) {
                        pollTimeout = Math.min(pollTimeout * 2, MAX_TIMEOUT);
                        // guarantees pending writes will be retired even though there is no new event to process
                        this.processPendingWrites();
                        continue;
                    }
                    pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
                }

                this.processPendingWrites();

                events.add(payloadRaw);
                this.consumerVms.transactionEvents.drainTo(events);
                this.sendBatchOfEvents(events);

            } catch (Exception e) {
                logger.log(ERROR, this.me.identifier+ ": Error captured in event loop \n"+e);
            }
        }
        logger.log(INFO, this.me.identifier+ "Finishing worker for consumer VMS: "+this.consumerVms.identifier);
    }

    private void processPendingWrites() {
        // do we have pending writes?
        ByteBuffer bb = this.PENDING_WRITES_BUFFER.poll();
        if (bb != null) {
            logger.log(INFO, me.identifier+": Retrying sending failed buffer to "+consumerVms.identifier);
            try {
                // sleep with the intention to let the OS flush the previous buffer
                try { sleep(100); } catch (InterruptedException ignored) { }
                this.WRITE_SYNCHRONIZER.take();
                this.connectionMetadata.channel.write(bb, networkSendTimeout, TimeUnit.MILLISECONDS, bb, this.writeCompletionHandler);
            } catch (Exception e) {
                logger.log(ERROR, me.identifier+": ERROR on retrying to send failed buffer to "+consumerVms.identifier+": \n"+e);
                if(e instanceof IllegalStateException){
                    logger.log(INFO, me.identifier+": Connection to "+consumerVms.identifier+" is open? "+connectionMetadata.channel.isOpen());
                    // probably comes from the class {@AsynchronousSocketChannelImpl}:
                    // "Writing not allowed due to timeout or cancellation"
                    stop();
                }

                if(this.WRITE_SYNCHRONIZER.isEmpty()){
                    this.WRITE_SYNCHRONIZER.add(DUMB);
                }

                bb.clear();
                boolean sent = this.PENDING_WRITES_BUFFER.offer(bb);
                while(!sent){
                    sent = this.PENDING_WRITES_BUFFER.offer(bb);
                }
            }
        }
    }

    private void sendBatchOfEvents(List<TransactionEvent.PayloadRaw> events) {
        int remaining = events.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
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
                writeBuffer.flip();

                this.WRITE_SYNCHRONIZER.take();
                this.connectionMetadata.channel.write(writeBuffer, this.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);

            } catch (Exception e) {
                logger.log(ERROR, this.me.identifier+ ": Error submitting events to "+this.consumerVms.identifier+"\n"+e);
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

                if(writeBuffer != null) {
                    this.returnByteBuffer(writeBuffer);
                }

                if(e instanceof IllegalStateException){
                    try { sleep(100); } catch (InterruptedException ignored) { }
                }

                // to avoid problems on future writes
                if(WRITE_SYNCHRONIZER.isEmpty()){
                    this.WRITE_SYNCHRONIZER.add(DUMB);
                }

                // force loop exit
                remaining = 0;
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

    private final class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {

        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            logger.log(DEBUG, me.identifier + ": Batch with size " + result + " has been sent to: " + consumerVms.identifier);
            if(byteBuffer.hasRemaining()){
                logger.log(WARNING, me.identifier + ": Remaining bytes will be sent to: " + consumerVms.identifier);
                // keep the lock and send the remaining
                connectionMetadata.channel.write(byteBuffer, networkSendTimeout, TimeUnit.MILLISECONDS, byteBuffer, this);
            } else {
                WRITE_SYNCHRONIZER.add(DUMB);
                returnByteBuffer(byteBuffer);
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            WRITE_SYNCHRONIZER.add(DUMB);
            logger.log(ERROR, me.identifier+": ERROR on writing batch of events to "+consumerVms.identifier+": \n"+exc);
            byteBuffer.clear();
            boolean sent = PENDING_WRITES_BUFFER.offer(byteBuffer);
            while(!sent){
                sent = PENDING_WRITES_BUFFER.offer(byteBuffer);
            }
            logger.log(INFO, me.identifier + ": Byte buffer added to pending queue. #pending: "+PENDING_WRITES_BUFFER.size());
        }
    }
}