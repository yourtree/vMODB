package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.web_common.meta.NetworkConfigConstants.DEFAULT_BUFFER_SIZE;
import static java.lang.Thread.sleep;

/**
 * This thread encapsulates the batch of events sending task
 * that should occur periodically. Once set up, it schedules itself
 * after each run, thus avoiding duplicate runs of the same task.
 * -
 * I could get the connection from the vms...
 * But in the future, one output event will no longer map to a single vms
 * So it is better to make the event sender task complete enough
 * FIXME what happens if the connection fails? then put the list of events
 *  in the batch to resend or return to the original location. let the
 *  main loop schedule the timer again. set the network node to off
 */
final class ConsumerVmsWorker extends TimerTask {

    private final VmsNode me;

    private final Logger logger;
    private final ConsumerVms consumerVms;
    private final ConnectionMetadata connectionMetadata;

    private final Deque<ByteBuffer> writeBufferPool;

    private final BlockingQueue<Byte> WRITE_SYNCHRONIZER = new ArrayBlockingQueue<>(1);

    private static final Byte DUMB = 1;

    public ConsumerVmsWorker(VmsNode me, ConsumerVms consumerVms, ConnectionMetadata connectionMetadata){
        this.me = me;
        this.consumerVms = consumerVms;
        this.connectionMetadata = connectionMetadata;
        this.logger = Logger.getLogger("vms-worker-"+consumerVms.hashCode());
        this.logger.setUseParentHandlers(true);
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(DEFAULT_BUFFER_SIZE) );
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(DEFAULT_BUFFER_SIZE) );

        // to allow the first thread to write
        this.WRITE_SYNCHRONIZER.add(DUMB);
    }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    @Override
    public void run() {

        // this.logger.info("VMS worker scheduled at: "+System.currentTimeMillis());

        // find the smallest batch. to avoid synchronizing with main thread
        // TODO maybe a skiplistmap is more helpful?
        long batchToSend = Long.MAX_VALUE;
        for(long batchId : this.consumerVms.transactionEventsPerBatch.keySet()){
            if(batchId < batchToSend) batchToSend = batchId;
        }

        if(batchToSend == Long.MAX_VALUE){
            return;
        }

        // there will always be a batch if this point of code is run
        // transient list due to concurrent threads.
        // if we have a fixed list, concurrent threads will contend for it
        // a temp list for each "run" can be discarded by the GC later without affecting concurrency
        List<TransactionEvent.Payload> events = new ArrayList<>(this.consumerVms.transactionEventsPerBatch.get(batchToSend).size());
        this.consumerVms.transactionEventsPerBatch.get(batchToSend).drainTo(events);
        int remaining = events.size();
        int count = remaining;
        ByteBuffer writeBuffer;
        while(remaining > 0){
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, events, writeBuffer);

                this.logger.info(me.vmsIdentifier+ ": Submitting "+(count - remaining)+" events from batch "+batchToSend+" to "+consumerVms);
                count = remaining;

                writeBuffer.flip();

                this.WRITE_SYNCHRONIZER.take();
                this.connectionMetadata.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);

                // prevent from error in consumer
                sleep_();

            } catch (Exception e) {
                this.logger.severe(me.vmsIdentifier+ ": Error submitting events from batch "+batchToSend+" to "+consumerVms);
                // return non-processed events to original location or what?
                if (!this.connectionMetadata.channel.isOpen()) {
                    this.logger.warning("The "+consumerVms+" VMS is offline");
                }
                // return events to the deque
                for (TransactionEvent.Payload event : events) {
                    this.consumerVms.transactionEventsPerBatch.get(batchToSend).offerFirst(event);
                }
            }
        }

    }

    private final Random random = new Random();

    /**
     * For some reason without sleeping, the bytebuffer gets corrupted in the consumer
     */
    private void sleep_(){
        // logger.info("Leader: Preparing another submission to: "+vmsNode.vmsIdentifier+" by thread "+Thread.currentThread().getId());
        // necessary to avoid buggy behavior: corrupted byte buffer. reason is unknown. maybe something related to operating system?
        try { sleep(random.nextInt(100)); } catch (InterruptedException ignored) {}
    }

    private ByteBuffer retrieveByteBuffer(){
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(DEFAULT_BUFFER_SIZE);
    }

    private class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer attachment) {
            logger.info(me.vmsIdentifier+ ": Batch with size "+result+" has been sent to: "+consumerVms);
            WRITE_SYNCHRONIZER.add(DUMB);
            attachment.clear();
            writeBufferPool.addLast( attachment );
        }
        @Override
        public void failed(Throwable exc, ByteBuffer attachment) {
            logger.severe(me.vmsIdentifier+": ERROR on writing batch of events to: "+consumerVms);
            WRITE_SYNCHRONIZER.add(DUMB);
            attachment.clear();
            writeBufferPool.addLast( attachment );
        }
    }
}