package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.transaction.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.IChannel;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
public final class ConsumerVmsWorker extends StoppableRunnable implements IVmsContainer {

    private static final System.Logger LOGGER = System.getLogger(ConsumerVmsWorker.class.getName());

    private static final VarHandle WRITE_SYNCHRONIZER;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            WRITE_SYNCHRONIZER = l.findVarHandle(ConsumerVmsWorker.class, "writeSynchronizer", int.class);
        } catch (Exception e) {
            throw new InternalError(e);
        }
    }

    @SuppressWarnings("unused")
    private volatile int writeSynchronizer;

    private final VmsNode me;

    private final IdentifiableNode consumerVms;
    
    private final IChannel channel;

    private final ILoggingHandler loggingHandler;

    private final IVmsSerdesProxy serdesProxy;

    private final Deque<ByteBuffer> writeBufferPool;

    private final VmsEventHandler.VmsHandlerOptions options;

    private final Queue<ByteBuffer> loggingWriteBuffers = new ConcurrentLinkedQueue<>();

    private final Deque<ByteBuffer> pendingWritesBuffer = new ConcurrentLinkedDeque<>();

    private final Queue<TransactionEvent.PayloadRaw> transactionEventQueue;

    private State state;

    protected enum State { NEW, CONNECTED, PRESENTATION_SENT, READY }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private final List<TransactionEvent.PayloadRaw> transactionEvents = new ArrayList<>(10000);

    public static ConsumerVmsWorker build(
                                  VmsNode me,
                                  IdentifiableNode consumerVms,
                                  Supplier<IChannel> channelSupplier,
                                  VmsEventHandler.VmsHandlerOptions options,
                                  ILoggingHandler loggingHandler,
                                  IVmsSerdesProxy serdesProxy) {
        return new ConsumerVmsWorker(me, consumerVms,
                channelSupplier.get(), options, loggingHandler, serdesProxy);
    }

    private ConsumerVmsWorker(VmsNode me,
                             IdentifiableNode consumerVms,
                             IChannel channel,
                             VmsEventHandler.VmsHandlerOptions options,
                             ILoggingHandler loggingHandler,
                             IVmsSerdesProxy serdesProxy) {
        this.me = me;
        this.consumerVms = consumerVms;
        this.channel = channel;
        this.loggingHandler = loggingHandler;
        this.serdesProxy = serdesProxy;
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize()) );
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize()) );
        this.options = options;
        this.transactionEventQueue = new ConcurrentLinkedQueue<>();
        this.state = NEW;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void acquireLock(){
        while(! WRITE_SYNCHRONIZER.compareAndSet(this, 0, 1) );
    }

    public boolean tryAcquireLock(){
        return WRITE_SYNCHRONIZER.compareAndSet(this, 0, 1);
    }

    public void releaseLock(){
        WRITE_SYNCHRONIZER.setVolatile(this, 0);
    }

    @Override
    public void run() {
        LOGGER.log(INFO, this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);
        if(!this.connect()) {
            LOGGER.log(WARNING, this.me.identifier+ ": Finishing worker for consumer VMS "+this.consumerVms.identifier+" because connection failed");
            return;
        }
        this.eventLoop();
        LOGGER.log(INFO, this.me.identifier+ ": Finishing worker for consumer VMS: "+this.consumerVms.identifier);
    }

    private void eventLoop() {
        int pollTimeout = 1;
        TransactionEvent.PayloadRaw payloadRaw;
        while(this.isRunning()){
            try {
                while((payloadRaw = this.transactionEventQueue.poll()) == null) {
                    pollTimeout = Math.min(pollTimeout * 2, this.options.maxSleep());
                    // guarantees pending writes will be retired even though there is no new event to process
                    this.processPendingWrites();
                    this.processPendingLogging();
                    this.giveUpCpu(pollTimeout);
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;

                // use first as estimation to avoid sending a single event
                int maxSize = this.options.networkBufferSize() - payloadRaw.totalSize();

//                if(!this.transactionEventQueue.isEmpty()){
                int sumTotal = 0;
                do {
                    this.transactionEvents.add(payloadRaw);
                    sumTotal += payloadRaw.totalSize();
                } while ( sumTotal < maxSize
                        || (payloadRaw = this.transactionEventQueue.poll()) != null);
                this.sendBatchOfEvents();
//                } else {
//                    this.sendEvent(payloadRaw);
//                }
                this.processPendingWrites();
                this.processPendingLogging();
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
            this.channel.write(buffer).get();
            this.state = PRESENTATION_SENT;
            LOGGER.log(DEBUG,me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" status = "+this.state);
            this.returnByteBuffer(buffer);
            LOGGER.log(INFO,me.identifier+ ": Setting up worker to send transactions to consumer VMS: "+this.consumerVms.identifier);
        } catch (Exception e) {
            // check if connection is still online. if so, try again
            // otherwise, retry connection in a few minutes
            LOGGER.log(ERROR, me.identifier + ": Caught an error while trying to connect to consumer VMS: " + this.consumerVms.identifier);
            return false;
        } finally {
            buffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(buffer);
        }
        this.state = READY;
        return true;
    }

    private void processPendingLogging(){
        ByteBuffer writeBuffer = this.loggingWriteBuffers.poll();
        if(writeBuffer != null){
            try {
                writeBuffer.position(0);
                this.loggingHandler.log(writeBuffer);
                // return buffer
                this.returnByteBuffer(writeBuffer);
            } catch (IOException e) {
                LOGGER.log(ERROR, me.identifier + ": Error on writing byte buffer to logging file: "+e.getMessage());
                this.loggingWriteBuffers.add(writeBuffer);
            }
        }
    }

    private void processPendingWrites() {
        // do we have pending writes?
        ByteBuffer bb = this.pendingWritesBuffer.poll();
        if (bb != null) {
            LOGGER.log(INFO, me.identifier+": Retrying sending failed buffer to "+consumerVms.identifier);
            try {
                // sleep with the intention to let the OS flush the previous buffer
                try { sleep(100); } catch (InterruptedException ignored) { }
                this.acquireLock();
                this.channel.write(bb, options.networkSendTimeout(), TimeUnit.MILLISECONDS, bb, this.writeCompletionHandler);
            } catch (Exception e) {
                LOGGER.log(ERROR, me.identifier+": ERROR on retrying to send failed buffer to "+consumerVms.identifier+": \n"+e);
                if(e instanceof IllegalStateException){
                    LOGGER.log(INFO, me.identifier+": Connection to "+consumerVms.identifier+" is open? "+this.channel.isOpen());
                    // probably comes from the class {@AsynchronousSocketChannelImpl}:
                    // "Writing not allowed due to timeout or cancellation"
                    stop();
                }
                this.releaseLock();
                bb.clear();
                this.pendingWritesBuffer.add(bb);
            }
        }
    }

    @SuppressWarnings("unused")
    private void sendEvent(TransactionEvent.PayloadRaw payload) {
        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        TransactionEvent.write( writeBuffer, payload );
        writeBuffer.flip();
        this.acquireLock();
        this.channel.write(writeBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
    }

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

                /* Blocking
                this.channel.write(writeBuffer).get();
                // drain buffer
                while(writeBuffer.hasRemaining()){
                    this.channel.write(writeBuffer).get();
                }
                this.returnByteBuffer(writeBuffer);
                */

                // maximize useful work
                while(!this.tryAcquireLock()){
                    this.processPendingLogging();
                }
                this.channel.write(writeBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
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
                this.releaseLock();

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
                releaseLock();
                if(options.logging()){
                    loggingWriteBuffers.add(byteBuffer);
                } else {
                    returnByteBuffer(byteBuffer);
                }
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            releaseLock();
            LOGGER.log(ERROR, me.identifier+": ERROR on writing batch of events to "+consumerVms.identifier+": \n"+exc);
            byteBuffer.position(0);
            pendingWritesBuffer.add(byteBuffer);
            LOGGER.log(INFO, me.identifier + ": Byte buffer added to pending queue. #pending: "+ pendingWritesBuffer.size());
        }
    }

    @Override
    public void queue(TransactionEvent.PayloadRaw eventPayload){
        this.transactionEventQueue.offer(eventPayload);
    }

    @Override
    public String identifier() {
        return this.consumerVms.identifier;
    }

}