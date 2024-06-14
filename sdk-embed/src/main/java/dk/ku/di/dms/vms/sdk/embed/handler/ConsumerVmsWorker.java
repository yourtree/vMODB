package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.sdk.embed.handler.ConsumerVmsWorker.State.*;
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

    private static final System.Logger LOGGER = System.getLogger(ConsumerVmsWorker.class.getName());
    
    private final VmsNode me;
    private final IdentifiableNode consumerVms;
    
    private final AsynchronousSocketChannel channel;

    private final IVmsSerdesProxy serdesProxy;

    private final Deque<ByteBuffer> writeBufferPool;

    private final BlockingQueue<Byte> WRITE_SYNCHRONIZER = new ArrayBlockingQueue<>(1);

    private static final Byte DUMB = 1;

    private final VmsEventHandler.VmsHandlerOptions options;

    private final Deque<ByteBuffer> PENDING_WRITES_BUFFER = new ConcurrentLinkedDeque<>();

    private final Queue<TransactionEvent.PayloadRaw> transactionEventQueue;

    private State state;

    public ConsumerVmsWorker(VmsNode me, IdentifiableNode consumerVms,
                             AsynchronousChannelGroup group, IVmsSerdesProxy serdesProxy,
                             VmsEventHandler.VmsHandlerOptions options) {
        this.me = me;
        this.consumerVms = consumerVms;
        try {
            this.channel = AsynchronousSocketChannel.open(group);
        } catch (IOException e) {
            LOGGER.log(ERROR, "Failed to open AsynchronousSocketChannel", e);
            throw new RuntimeException(e);
        }
        this.serdesProxy = serdesProxy;

        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize()) );
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize()) );

        // to allow the first thread to write
        this.WRITE_SYNCHRONIZER.add(DUMB);

        this.options = options;
        
        this.transactionEventQueue = new ConcurrentLinkedQueue<>();

        this.state = NEW;
    }

    protected enum State { NEW, CONNECTED, PRESENTATION_SENT, READY }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    @Override
    public void run() {
        LOGGER.log(INFO, this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);
        if(!this.connect()) {
            LOGGER.log(WARNING, this.me.identifier+ "Finishing worker for consumer VMS: "+this.consumerVms.identifier+" because connection failed");
            return;
        }
        this.eventLoop();
        LOGGER.log(INFO, this.me.identifier+ "Finishing worker for consumer VMS: "+this.consumerVms.identifier);
    }

    @SuppressWarnings("BusyWait")
    private void eventLoop() {
        int pollTimeout = 1;
        TransactionEvent.PayloadRaw payloadRaw;
        while(this.isRunning()){
            try {
                payloadRaw = this.transactionEventQueue.poll();
                if (payloadRaw == null) {
                    pollTimeout = Math.min(pollTimeout * 2, this.options.maxSleep());
                    // guarantees pending writes will be retired even though there is no new event to process
                    this.processPendingWrites();
                    sleep(pollTimeout);
                    continue;
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;

//                if(!this.transactionEventQueue.isEmpty()){
                    do {
                        this.transactionEvents.add(payloadRaw);
                    } while ((payloadRaw = this.transactionEventQueue.poll()) != null);
                    this.sendBatchOfEvents();
//                } else {
//                    this.sendEvent(payloadRaw);
//                }
                this.processPendingWrites();
            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+ ": Error captured in event loop \n"+e);
            }
        }
    }

    /**
     * Responsible for making sure the handshake protocol is
     * successfully performed with a consumer VMS
     */
    private boolean connect() {
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize());
        try{
            NetworkUtils.configure(this.channel, options.osBufferSize());
            this.channel.connect(this.consumerVms.asInetSocketAddress()).get();

            this.state = CONNECTED;
            LOGGER.log(DEBUG,this.me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" status = "+this.state);

            String dataSchema = this.serdesProxy.serializeDataSchema(this.me.dataSchema);
            String inputEventSchema = this.serdesProxy.serializeEventSchema(this.me.inputEventSchema);
            String outputEventSchema = this.serdesProxy.serializeEventSchema(this.me.outputEventSchema);

            buffer.clear();
            Presentation.writeVms( buffer, this.me, this.me.identifier, this.me.batch, 0, this.me.previousBatch, dataSchema, inputEventSchema, outputEventSchema );
            buffer.flip();

            channel.write(buffer).get();

            this.state = PRESENTATION_SENT;

            LOGGER.log(DEBUG,me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" status = "+this.state);

            this.returnByteBuffer(buffer);

            LOGGER.log(INFO,me.identifier+ " setting up worker to send transactions to consumer VMS: "+this.consumerVms.identifier);

        } catch (Exception e) {
            // check if connection is still online. if so, try again
            // otherwise, retry connection in a few minutes
            LOGGER.log(ERROR, me.identifier + "caught an error while trying to connect to consumer VMS: " + this.consumerVms.identifier);
            return false;
        } finally {
            buffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(buffer);
        }
        this.state = READY;
        return true;
    }

    private void processPendingWrites() {
        // do we have pending writes?
        ByteBuffer bb = this.PENDING_WRITES_BUFFER.poll();
        if (bb != null) {
            LOGGER.log(INFO, me.identifier+": Retrying sending failed buffer to "+consumerVms.identifier);
            try {
                // sleep with the intention to let the OS flush the previous buffer
                try { sleep(100); } catch (InterruptedException ignored) { }
                this.WRITE_SYNCHRONIZER.take();
                this.channel.write(bb, options.networkSendTimeout(), TimeUnit.MILLISECONDS, bb, this.writeCompletionHandler);
            } catch (Exception e) {
                LOGGER.log(ERROR, me.identifier+": ERROR on retrying to send failed buffer to "+consumerVms.identifier+": \n"+e);
                if(e instanceof IllegalStateException){
                    LOGGER.log(INFO, me.identifier+": Connection to "+consumerVms.identifier+" is open? "+this.channel.isOpen());
                    // probably comes from the class {@AsynchronousSocketChannelImpl}:
                    // "Writing not allowed due to timeout or cancellation"
                    stop();
                }

                if(this.WRITE_SYNCHRONIZER.isEmpty()){
                    this.WRITE_SYNCHRONIZER.add(DUMB);
                }

                bb.clear();
                this.PENDING_WRITES_BUFFER.add(bb);
            }
        }
    }

    private void sendEvent(TransactionEvent.PayloadRaw payload) throws InterruptedException {
        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        TransactionEvent.write( writeBuffer, payload );
        writeBuffer.flip();
        this.WRITE_SYNCHRONIZER.take();
        this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
    }

    private final List<TransactionEvent.PayloadRaw> transactionEvents = new ArrayList<>(1024);

    private void sendBatchOfEvents() {
        int remaining = this.transactionEvents.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        while(remaining > 0){
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, this.transactionEvents, writeBuffer);

                LOGGER.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;

                writeBuffer.flip();

                this.channel.write(writeBuffer).get();
                // drain buffer
                while(writeBuffer.hasRemaining()){
                    // LOGGER.log(WARNING, "Here we gooooo");
                    this.channel.write(writeBuffer).get();
                }
                this.returnByteBuffer(writeBuffer);

//                this.WRITE_SYNCHRONIZER.take();
//                this.channel.write(writeBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);

            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+ ": Error submitting events to "+this.consumerVms.identifier+"\n"+e);
                // return non-processed events to original location or what?
                if (!this.channel.isOpen()) {
                    LOGGER.log(WARNING, "The "+this.consumerVms.identifier+" VMS is offline");
                }

                // return events to the deque
                while(!this.transactionEvents.isEmpty()) {
                    if(this.transactionEventQueue.offer(this.transactionEvents.get(0))){
                        this.transactionEvents.remove(0);
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
        this.transactionEvents.clear();
    }

    private ByteBuffer retrieveByteBuffer(){
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize());
    }

    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        this.writeBufferPool.add(bb);
    }

    private final class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {

        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            LOGGER.log(DEBUG, me.identifier + ": Batch with size " + result + " has been sent to: " + consumerVms.identifier);
            if(byteBuffer.hasRemaining()){
                LOGGER.log(WARNING, me.identifier + ": Remaining bytes will be sent to: " + consumerVms.identifier);
                // keep the lock and send the remaining
                channel.write(byteBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, byteBuffer, this);
            } else {
                WRITE_SYNCHRONIZER.add(DUMB);
                returnByteBuffer(byteBuffer);
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            WRITE_SYNCHRONIZER.add(DUMB);
            LOGGER.log(ERROR, me.identifier+": ERROR on writing batch of events to "+consumerVms.identifier+": \n"+exc);
            byteBuffer.clear();
            PENDING_WRITES_BUFFER.add(byteBuffer);
            LOGGER.log(INFO, me.identifier + ": Byte buffer added to pending queue. #pending: "+PENDING_WRITES_BUFFER.size());
        }
    }

    public void queueTransactionEvent(TransactionEvent.PayloadRaw eventPayload){
        this.transactionEventQueue.offer(eventPayload);
    }
    
}