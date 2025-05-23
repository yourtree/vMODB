package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.IChannel;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;

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

    private static final Deque<ByteBuffer> WRITE_BUFFER_POOL = new ConcurrentLinkedDeque<>();

    private final VmsEventHandler.VmsHandlerOptions options;

    private final Queue<ByteBuffer> loggingWriteBuffers = new ConcurrentLinkedQueue<>();

    private final Deque<ByteBuffer> pendingWritesBuffer = new ConcurrentLinkedDeque<>();

    private final MpscBlockingConsumerArrayQueue<TransactionEvent.PayloadRaw> transactionEventQueue;

    private State state;

    protected enum State { NEW, CONNECTED, PRESENTATION_SENT, READY }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private final List<TransactionEvent.PayloadRaw> drained = new ArrayList<>(1024*10);

    public static ConsumerVmsWorker build(
                                  VmsNode me,
                                  IdentifiableNode consumerVms,
                                  Supplier<IChannel> channelSupplier,
                                  VmsEventHandler.VmsHandlerOptions options,
                                  IVmsSerdesProxy serdesProxy) {
        return new ConsumerVmsWorker(me, consumerVms,
                channelSupplier.get(), options, serdesProxy);
    }

    private ConsumerVmsWorker(VmsNode me,
                             IdentifiableNode consumerVms,
                             IChannel channel,
                             VmsEventHandler.VmsHandlerOptions options,
                             IVmsSerdesProxy serdesProxy) {
        this.me = me;
        this.consumerVms = consumerVms;
        this.channel = channel;

        ILoggingHandler loggingHandler;
        var logIdentifier = me.identifier+"_"+consumerVms.identifier;
        if(options.logging()){
            loggingHandler = LoggingHandlerBuilder.build(logIdentifier);
        } else {
            String loggingStr = System.getProperty("logging");
            if(Boolean.parseBoolean(loggingStr)){
                loggingHandler = LoggingHandlerBuilder.build(logIdentifier);
            } else {
                loggingHandler = new ILoggingHandler() { };
            }
        }

        this.loggingHandler = loggingHandler;
        this.serdesProxy = serdesProxy;
        this.options = options;
        this.transactionEventQueue = new MpscBlockingConsumerArrayQueue<>(1024*1500);
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
        // System.out.println(me.identifier +": Releasing lock");
        WRITE_SYNCHRONIZER.setVolatile(this, 0);
    }

    @Override
    public void run() {
        LOGGER.log(INFO, this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);
        if(!this.connect()) {
            LOGGER.log(WARNING, this.me.identifier+ ": Finishing prematurely worker for consumer VMS "+this.consumerVms.identifier+" because connection failed");
            return;
        }
        if(this.options.logging()){
            this.eventLoopLogging();
        } else {
            this.eventLoopNoLogging();
        }
        LOGGER.log(INFO, this.me.identifier+ ": Finishing worker for consumer VMS: "+this.consumerVms.identifier);
    }

    private void eventLoopNoLogging() {
        while(this.isRunning()){
            try {
                this.drained.add(this.transactionEventQueue.take());
                this.transactionEventQueue.drain(this.drained::add);
                if(this.drained.size() == 1){
                    this.sendEventBlocking(this.drained.removeFirst());
                } else {
                    this.sendBatchOfEventsBlocking();
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+ ": Error captured in event loop (no logging) \n"+e);
            }
        }
    }

    private void eventLoopLogging() {
        while(this.isRunning()){
            try {
                if(this.loggingWriteBuffers.isEmpty()){
                    //LOGGER.log(WARNING, me.identifier+": Going to block since no logging buffers");
                    this.drained.add(this.transactionEventQueue.take());
                    //LOGGER.log(WARNING, me.identifier+": Woke up");
                }
                this.transactionEventQueue.drain(this.drained::add);
                if(this.drained.isEmpty()){
                    this.processPendingLogging();
                } else {
                    this.sendBatchOfEventsNonBlocking();
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+ ": Error captured in event loop (logging) \n"+e);
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
            NetworkUtils.configure(this.channel, options.soBufferSize());
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
        ByteBuffer writeBuffer;
        if((writeBuffer = this.loggingWriteBuffers.poll())!= null){
            try {
                writeBuffer.position(0);
                this.loggingHandler.log(writeBuffer);
                this.returnByteBuffer(writeBuffer);
            } catch (Exception e) {
                LOGGER.log(ERROR, me.identifier + ": Error on writing byte buffer to logging file: "+e.getMessage());
                e.printStackTrace(System.out);
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
                    this.stop();
                }
                this.releaseLock();
                bb.clear();
                this.pendingWritesBuffer.add(bb);
            }
        }
    }

    private void sendBatchOfEventsNonBlocking() {
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        while(remaining > 0){
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, this.drained, writeBuffer);
                writeBuffer.flip();
                LOGGER.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;
                // maximize useful work
                while(!this.tryAcquireLock()){
                    this.processPendingLogging();
                }
                this.channel.write(writeBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                remaining = 0;
                this.releaseLock();
            }
        }
        this.drained.clear();
    }

    private void sendEventBlocking(TransactionEvent.PayloadRaw payload) {
        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        try {
            TransactionEvent.write( writeBuffer, payload );
            writeBuffer.flip();
            do {
                this.channel.write(writeBuffer).get();
            } while (writeBuffer.hasRemaining());
        } catch (Exception e){
            if(this.isRunning()) {
                LOGGER.log(ERROR, "Error caught on sending single event: " + e);
                this.transactionEventQueue.offer(payload);
            }
            writeBuffer.clear();
        }
        finally {
            this.returnByteBuffer(writeBuffer);
        }
    }

    private void sendBatchOfEventsBlocking() {
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        while(remaining > 0){
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, this.drained, writeBuffer);
                writeBuffer.flip();
                LOGGER.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;
                do {
                    this.channel.write(writeBuffer).get();
                } while(writeBuffer.hasRemaining());
                this.returnByteBuffer(writeBuffer);
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                // force loop exit
                remaining = 0;
            }
        }
        this.drained.clear();
    }

    private void failSafe(Exception e, ByteBuffer writeBuffer) {
        LOGGER.log(ERROR, this.me.identifier+ ": Error submitting events to "+this.consumerVms.identifier+"\n"+ e);
        // return non-processed events to original location or what?
        if (!this.channel.isOpen()) {
            LOGGER.log(WARNING, "The "+this.consumerVms.identifier+" VMS is offline");
        }
        // return events to the deque
        this.transactionEventQueue.addAll(this.drained);
        if(writeBuffer != null) {
            this.returnByteBuffer(writeBuffer);
        }
    }

    private ByteBuffer retrieveByteBuffer(){
        ByteBuffer bb = WRITE_BUFFER_POOL.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(this.options.networkBufferSize());
    }

    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        WRITE_BUFFER_POOL.add(bb);
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
                if(options.logging()){
                    loggingWriteBuffers.add(byteBuffer);
                } else {
                    returnByteBuffer(byteBuffer);
                }
                releaseLock();
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
        if(!this.transactionEventQueue.offer(eventPayload)){
            System.out.println(me.identifier +": cannot add event in the input queue");
        }
    }

    @Override
    public String identifier() {
        return this.consumerVms.identifier;
    }

    @Override
    public void stop(){
        super.stop();
        this.channel.close();
    }

}