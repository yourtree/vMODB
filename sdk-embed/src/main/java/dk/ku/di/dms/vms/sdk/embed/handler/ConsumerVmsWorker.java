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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

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

    @Override
    public void run() {

        logger.log(INFO, this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);

        int pollTimeout = 50;
        TransactionEvent.PayloadRaw payloadRaw;
        List<TransactionEvent.PayloadRaw> events = new ArrayList<>(1000);
        while(this.isRunning()){

            try {
                payloadRaw = this.consumerVms.transactionEvents.poll(pollTimeout, TimeUnit.MILLISECONDS);
                if (payloadRaw == null) {
                    pollTimeout = pollTimeout * 2;
                    continue;
                }
            } catch (InterruptedException ignored) {
                continue;
            }
            pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
            events.add(payloadRaw);

            this.consumerVms.transactionEvents.drainTo(events);

            int remaining = events.size();
            int count = remaining;
            ByteBuffer writeBuffer;
            while(remaining > 0){
                try {
                    writeBuffer = this.retrieveByteBuffer();
                    remaining = BatchUtils.assembleBatchPayload(remaining, events, writeBuffer);

                    logger.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                    count = remaining;

                    writeBuffer.flip();

                    this.WRITE_SYNCHRONIZER.take();
                    this.connectionMetadata.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
                } catch (Exception e) {
                    logger.log(ERROR, this.me.identifier+ ": Error submitting events to "+this.consumerVms.identifier);
                    // return non-processed events to original location or what?
                    if (!this.connectionMetadata.channel.isOpen()) {
                        logger.log(WARNING, "The "+this.consumerVms.identifier+" VMS is offline");
                    }
                    // return events to the deque
                    for (TransactionEvent.PayloadRaw event : events) {
                        this.consumerVms.transactionEvents.offerFirst(event);
                    }
                }
            }
            events.clear();
        }
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

    private class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {

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
            if(byteBuffer.hasRemaining()){
                logger.log(ERROR, me.identifier + " on failed. Consumer worker for "+consumerVms.identifier+" found not all bytes were sent!");
            }
            logger.log(ERROR, me.identifier+": ERROR on writing batch of events to: "+consumerVms.identifier);
            WRITE_SYNCHRONIZER.add(DUMB);
            returnByteBuffer(byteBuffer);
        }

    }
}