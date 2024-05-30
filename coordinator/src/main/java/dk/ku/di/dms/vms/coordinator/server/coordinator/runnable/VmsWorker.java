package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.VmsWorker.State.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static java.lang.System.Logger.Level.*;
import static java.lang.Thread.sleep;

@SuppressWarnings("SequencedCollectionMethodCanBeUsed")
public final class VmsWorker extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(VmsWorker.class.getName());
    
    private final ServerNode me;

    // the vms this worker is responsible for
    private IdentifiableNode consumerVms;

    private final int networkBufferSize;

    private final int networkSendTimeout;

    private State state;

    private final IVmsSerdesProxy serdesProxy;

    private final ByteBuffer readBuffer;

    private final Deque<ByteBuffer> writeBufferPool;

    /**
     * Queues to inform coordinator about an event
     */
    private final BlockingQueue<Object> coordinatorQueue;

    private final AsynchronousSocketChannel channel;

    // DTs particular to this vms worker
    // private final BlockingDeque<TransactionEvent.PayloadRaw> transactionEventQueue;

    private final Queue<TransactionEvent.PayloadRaw> transactionEventQueue;

    private final Queue<Object> pendingMessages;

    // could be switched by a semaphore
    private final BlockingQueue<Byte> WRITE_SYNCHRONIZER = new ArrayBlockingQueue<>(1);

    private static final Byte DUMB = 1;

    private final boolean initHandshake;

    static VmsWorker buildAsStarter(// coordinator reference
                                    ServerNode me,
                                    // the vms this thread is responsible for
                                    IdentifiableNode consumerVms,
                                    // shared data structure to communicate messages to coordinator
                                    BlockingQueue<Object> coordinatorQueue,
                                    // the group for the socket channel
                                    AsynchronousChannelGroup group,
                                    int networkBufferSize,
                                    int networkSendTimeout,
                                    IVmsSerdesProxy serdesProxy) throws IOException {
        // passing null is clearly not a good option. when it comes to support VMS node crashes, it will be necessary to rethink this design
        return new VmsWorker(me, consumerVms, coordinatorQueue, AsynchronousSocketChannel.open(group),
                MemoryManager.getTemporaryDirectBuffer(networkBufferSize),
                networkBufferSize, networkSendTimeout, serdesProxy, true);
    }

    static VmsWorker build(ServerNode me,
                            IdentifiableNode consumerVms,
                            BlockingQueue<Object> coordinatorQueue,
                            AsynchronousChannelGroup group,
                            int networkBufferSize,
                            int networkSendTimeout,
                            IVmsSerdesProxy serdesProxy) throws IOException {
        // passing null is clearly not a good option. when it comes to support VMS node crashes, it will be necessary to rethink this design
        return new VmsWorker(me, consumerVms, coordinatorQueue, AsynchronousSocketChannel.open(group),
                MemoryManager.getTemporaryDirectBuffer(networkBufferSize),
                networkBufferSize, networkSendTimeout, serdesProxy, false);
    }

    private VmsWorker(// coordinator reference
                      ServerNode me,
                      // the vms this thread is responsible for
                      IdentifiableNode consumerVms,
                      // events to share with coordinator
                      BlockingQueue<Object> coordinatorQueue,
                      AsynchronousSocketChannel channel,
                      ByteBuffer readBuffer,
                      int networkBufferSize,
                      int networkSendTimeout,
                      IVmsSerdesProxy serdesProxy,
                      boolean initHandshake) {
        this.me = me;
        this.state = State.NEW;
        this.consumerVms = consumerVms;

        this.channel = channel;

        this.networkBufferSize = networkBufferSize;
        this.networkSendTimeout = networkSendTimeout;

        // initialize the write buffer
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        // populate buffer pool
        this.writeBufferPool.addFirst( retrieveByteBuffer() );
        this.writeBufferPool.addFirst( retrieveByteBuffer() );

        this.readBuffer = readBuffer;
        this.serdesProxy = serdesProxy;

        // to allow the first thread to write
        this.WRITE_SYNCHRONIZER.add(DUMB);

        // in
        this.pendingMessages = new ConcurrentLinkedQueue<>();
        this.transactionEventQueue = new ConcurrentLinkedQueue<>();

        // out - shared by many vms workers
        this.coordinatorQueue = coordinatorQueue;

        this.initHandshake = initHandshake;
    }

    private void connect() throws IOException, InterruptedException, ExecutionException {
        NetworkUtils.configure(this.channel, networkBufferSize);
        this.channel.connect(this.consumerVms.asInetSocketAddress()).get();
    }

    public void initHandshakeProtocol(){
        try{
            LOGGER.log(INFO, "Leader: Attempting connection to "+this.consumerVms.identifier);
            this.connect();

            this.state = CONNECTION_ESTABLISHED;
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            this.sendLeaderPresentationToVms(writeBuffer);
            this.state = State.LEADER_PRESENTATION_SENT;

            // set read handler here
            this.channel.read( this.readBuffer, 0, new VmsReadCompletionHandler() );

        } catch (Exception e) {
            LOGGER.log(WARNING,"Failed to connect to a known VMS: " + this.consumerVms.identifier);

            if(this.WRITE_SYNCHRONIZER.isEmpty()){
                this.WRITE_SYNCHRONIZER.add(DUMB);
            }

            if (this.state == State.NEW) {
                // forget about it, let the vms connect then...
                this.state = State.CONNECTION_FAILED;
            } else if(this.state == CONNECTION_ESTABLISHED) {
                this.state = LEADER_PRESENTATION_SEND_FAILED;
                // check if connection is still online. if so, try again
                // otherwise, retry connection in a few minutes
                if(this.channel.isOpen()){
                    // try again? what is the problem?
                    LOGGER.log(WARNING,"It was not possible to send a presentation message, although the channel is open. The connection will be closed now.");
                    try { this.channel.close(); } catch (IOException ignored) { }
                } else {
                    LOGGER.log(WARNING,"It was not possible to send a presentation message and the channel is not open. Check the consumer VMS: " + consumerVms);
                }
            } else {
                LOGGER.log(WARNING,"Cannot find the root problem. Please have a look: "+e.getCause().getMessage());
            }

            // important for consistency of state (if debugging, good to see the code controls the thread state)
            this.stop();
        }
    }

    // write presentation
    private void sendLeaderPresentationToVms(ByteBuffer writeBuffer) throws InterruptedException {
        Presentation.writeServer(writeBuffer, this.me, true);
        writeBuffer.flip();
        this.WRITE_SYNCHRONIZER.take();
        this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
    }

    @Override
    public void run() {
        LOGGER.log(INFO, "VmsWorker starting for consumer VMS: \n"+consumerVms);
        if(this.initHandshake) {
            this.initHandshakeProtocol();
        } else {
            if (this.initSimpleConnection()) return;
        }
        this.eventLoop();
        LOGGER.log(INFO, "VmsWorker finished for consumer VMS: \n"+consumerVms);
    }

    private boolean initSimpleConnection() {
        // only connect. the presentation has already been sent
        LOGGER.log(INFO, Thread.currentThread().getName()+": Attempting additional connection to to VMS: " + this.consumerVms.identifier);
        try {
            this.connect();
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            this.sendLeaderPresentationToVms(writeBuffer);
        } catch(Exception ignored){
            LOGGER.log(ERROR, Thread.currentThread().getName()+": Cannot connect to VMS: " + this.consumerVms.identifier);
            return true;
        }
        LOGGER.log(INFO, Thread.currentThread().getName()+": Additional connection to "+this.consumerVms.identifier+" succeeded");
        return false;
    }

    private static final int MAX_SLEEP = 1000;

    /**
     * Event loop performs two tasks:
     * (a) get and process batch-tracking messages from coordinator
     * (b) process transaction input events
     */
    @SuppressWarnings("BusyWait")
    private void eventLoop() {
        int pollTimeout = 1;
        TransactionEvent.PayloadRaw payloadRaw;
        while (this.isRunning()){
            try {
                payloadRaw = this.transactionEventQueue.poll();
                if(payloadRaw == null){
                    pollTimeout = Math.min(pollTimeout * 2, MAX_SLEEP);
                    this.processPendingTasks();
                    sleep(pollTimeout);
                    continue;
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;

                if(!this.transactionEventQueue.isEmpty()){
                    do {
                        this.transactionEvents.add(payloadRaw);
                    } while ((payloadRaw = this.transactionEventQueue.poll()) != null);
                    this.sendBatchOfEvents();
                } else {
                    this.sendEvent(payloadRaw);
                }

                this.processPendingTasks();

            } catch (InterruptedException e) {
                LOGGER.log(ERROR, "Leader: VMS worker for "+this.consumerVms.identifier+" has been interrupted: "+e);
                this.stop();
            } catch (Exception e) {
                LOGGER.log(ERROR, "Leader: VMS worker for "+this.consumerVms.identifier+" has caught an exception: "+e);
            }
        }
    }

    private void processPendingTasks() {

        Object msg;
        while((msg = this.pendingMessages.poll()) != null){
            this.sendMessage(msg);
        }

        ByteBuffer bb = this.PENDING_WRITES_BUFFER.poll();
        if(bb != null){
            LOGGER.log(INFO, "Leader: Retrying sending failed buffer send");
            try {
                this.WRITE_SYNCHRONIZER.take();
                this.channel.write(bb, networkSendTimeout, TimeUnit.MILLISECONDS, bb, this.batchWriteCompletionHandler);
            } catch (Exception e){
                LOGGER.log(ERROR, "Leader: ERROR on retrying to send failed buffer: \n"+e);
                if(e instanceof IllegalStateException){
                    // probably comes from the class {@AsynchronousSocketChannelImpl}:
                    // "Writing not allowed due to timeout or cancellation"
                    // stop thread since there is no way to write to this channel anymore
                    this.stop();
                }

                if(this.WRITE_SYNCHRONIZER.isEmpty()){
                    this.WRITE_SYNCHRONIZER.add(DUMB);
                }

                bb.clear();
                this.PENDING_WRITES_BUFFER.offer(bb);

            }
        }

    }

    public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
        this.transactionEventQueue.offer(payloadRaw);
    }

    private void sendEvent(TransactionEvent.PayloadRaw payload) throws InterruptedException {
        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        TransactionEvent.write( writeBuffer, payload );
        writeBuffer.flip();
        this.WRITE_SYNCHRONIZER.take();
        this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
    }

    public void queueMessage(Object message) {
        this.pendingMessages.offer(message);
    }

    private void sendMessage(Object message) {
        switch (message) {
            case BatchCommitCommand.Payload o -> this.sendBatchCommitCommand(o);
            case BatchCommitInfo.Payload o -> this.sendBatchCommitInfo(o);
            case TransactionAbort.Payload o -> this.sendTransactionAbort(o);
            case String o -> this.sendConsumerSet(o);
            default ->
                    LOGGER.log(WARNING, "Leader: VMS worker for " + this.consumerVms.identifier + " has unknown message type: " + message.getClass().getName());
        }
    }

    private void sendTransactionAbort(TransactionAbort.Payload tidToAbort) {
        try {
            ByteBuffer writeBuffer = retrieveByteBuffer();
            TransactionAbort.write(writeBuffer, tidToAbort);
            writeBuffer.flip();
            this.WRITE_SYNCHRONIZER.take();
            this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
            LOGGER.log(WARNING,"Leader: Transaction abort sent to: " + this.consumerVms.identifier);
        } catch (InterruptedException e){
            LOGGER.log(ERROR,"Leader: Transaction abort write has failed:\n"+e.getMessage());
            if(!this.channel.isOpen()){
                LOGGER.log(WARNING,"Leader: Channel with "+this.consumerVms.identifier+" is closed");
                this.stop(); // no reason to continue the loop
            }
            this.pendingMessages.add(tidToAbort);
        }
    }

    private void sendBatchCommitCommand(BatchCommitCommand.Payload batchCommitCommand) {
        try {
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            BatchCommitCommand.write(writeBuffer, batchCommitCommand);
            writeBuffer.flip();
            this.WRITE_SYNCHRONIZER.take();
            this.channel.write(writeBuffer, this.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            LOGGER.log(INFO, "Leader: Batch ("+batchCommitCommand.batch()+") commit command sent to: " + this.consumerVms.identifier);
        } catch (Exception e){
            LOGGER.log(ERROR,"Leader: Batch ("+batchCommitCommand.batch()+") commit command write has failed:\n"+e.getMessage());
            if(!this.channel.isOpen()){
                LOGGER.log(WARNING,"Leader: Channel with "+this.consumerVms.identifier+"is closed");
                this.stop(); // no reason to continue the loop
            }
            this.pendingMessages.add(batchCommitCommand);
            if(this.WRITE_SYNCHRONIZER.isEmpty()){
                this.WRITE_SYNCHRONIZER.add(DUMB);
            }
        }
    }

    private void sendConsumerSet(String vmsConsumerSet) {
        // the first or new information
        if(this.state == VMS_PRESENTATION_PROCESSED) {
            this.state = CONSUMER_SET_READY_FOR_SENDING;
            LOGGER.log(INFO, "Leader: Consumer set will be sent to: "+this.consumerVms.identifier);
        } else if(this.state == CONSUMER_EXECUTING){
            LOGGER.log(INFO, "Leader: Consumer set is going to be updated for: "+this.consumerVms.identifier);
        } else if(this.state == CONSUMER_SET_SENDING_FAILED){
            LOGGER.log(INFO, "Leader: Consumer set, another attempt to write to: "+this.consumerVms.identifier);
        } // else, nothing...

        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        try {
            ConsumerSet.write(writeBuffer, vmsConsumerSet);
            writeBuffer.flip();
            this.WRITE_SYNCHRONIZER.take();
            this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
            if (this.state == CONSUMER_SET_READY_FOR_SENDING) {// or != CONSUMER_EXECUTING
                this.state = CONSUMER_EXECUTING;
            }
        } catch (InterruptedException e){
            this.state = CONSUMER_SET_SENDING_FAILED;
            LOGGER.log(WARNING,"Write has failed", e);
            this.stop(); // no reason to continue the loop
        } catch (IOException | BufferOverflowException e){
            this.state = CONSUMER_SET_SENDING_FAILED;
            LOGGER.log(WARNING,"Write has failed and the VMS worker will undergo an unknown state: " + consumerVms, e);
            this.stop(); // no reason to continue the loop
        }
    }

    /**
     * Reuses the thread from the socket thread pool, instead of assigning a specific thread
     * Removes thread context switching costs.
     * This thread should not block.
     * The idea is to decode the message and deliver back to socket loop as soon as possible
     * This thread must be set free as soon as possible, should not do long-running computation
     */
    private final class VmsReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        @Override
        public void completed(Integer result, Integer startPos) {

            if(result == -1){
                LOGGER.log(INFO, "Leader: " + consumerVms.identifier+" has disconnected");
                return;
            }

            if(startPos == 0){
                readBuffer.flip();
            }

            // decode message by getting the first byte
            readBuffer.position(startPos);
            byte type = readBuffer.get();

            try {
                switch (type) {
                    case PRESENTATION -> {
                        // this is a very bad conditional statement
                        // we could do better removing this concept of "unknown" and simply check the state
                        if (state == LEADER_PRESENTATION_SENT) {
                            state = VMS_PRESENTATION_RECEIVED;// for the first time
                            processVmsIdentifier();
                            state = VMS_PRESENTATION_PROCESSED;
                        } else {
                            // in the future it can be an update of the vms schema or crash recovery
                            LOGGER.log(ERROR, "Leader: Presentation already received from VMS: " + consumerVms.identifier);
                        }
                    }
                    // from all terminal VMSs involved in the last batch
                    case BATCH_COMPLETE -> {
                        // don't actually need the host and port in the payload since we have the attachment to this read operation...
                        BatchComplete.Payload response = BatchComplete.read(readBuffer);
                        LOGGER.log(DEBUG, "Leader: Batch (" + response.batch() + ") complete received from: " + consumerVms.identifier);
                        // must have a context, i.e., what batch, the last?
                        coordinatorQueue.add(response);
                        // if one abort, no need to keep receiving
                        // actually it is unclear in which circumstances a vms would respond no... probably in case it has not received an ack from an aborted commit response?
                        // because only the aborted transaction will be rolled back
                    }
                    case BATCH_COMMIT_ACK -> {
                        LOGGER.log(DEBUG, "Leader: Batch commit ACK received from: " + consumerVms.identifier);
                        BatchCommitAck.Payload response = BatchCommitAck.read(readBuffer);
                        // logger.config("Just logging it, since we don't necessarily need to wait for that. "+response);
                        coordinatorQueue.add(response);
                    }
                    case TX_ABORT -> {
                        // get information of what
                        TransactionAbort.Payload response = TransactionAbort.read(readBuffer);
                        coordinatorQueue.add(response);
                    }
                    case EVENT -> LOGGER.log(INFO, "Leader: New event received from: " + consumerVms.identifier);
                    case BATCH_OF_EVENTS -> LOGGER.log(INFO, "Leader: New batch of events received from VMS");
                    default -> LOGGER.log(WARNING, "Leader: Unknown message received.");

                }
            } catch (BufferUnderflowException e){
                LOGGER.log(WARNING, "Leader: Buffer underflow captured. Will read more with the hope the full data is delivered.");
                e.printStackTrace(System.out);
                readBuffer.position(startPos);
                readBuffer.compact();
                channel.read( readBuffer, startPos, this );
                return;
            } catch (Exception e){
                LOGGER.log(ERROR, "Leader: Unknown error captured:"+e.getMessage(), e);
                e.printStackTrace(System.out);
                return;
            }

            if(readBuffer.hasRemaining()){
                this.completed(result, readBuffer.position());
            } else {
                readBuffer.clear();
                channel.read( readBuffer, 0, this );
            }
        }

        @Override
        public void failed(Throwable exc, Integer startPosition) {
            if(state == LEADER_PRESENTATION_SENT){
                state = VMS_PRESENTATION_RECEIVE_FAILED;
                LOGGER.log(WARNING,"It was not possible to receive a presentation message from consumer VMS: "+exc.getMessage());
            } else {
                if (channel.isOpen()) {
                    LOGGER.log(WARNING,"Read has failed but channel is open. Trying to read again from: " + consumerVms);
                } else {
                    LOGGER.log(WARNING,"Read has failed and channel is closed: " + consumerVms);
                }
            }
            readBuffer.clear();
            channel.read(readBuffer, 0, this);
        }
    }

    private void processVmsIdentifier() {
        // always a vms
        this.readBuffer.position(2);
        this.consumerVms = Presentation.readVms(readBuffer, serdesProxy);
        this.state = State.VMS_PRESENTATION_PROCESSED;
        // let coordinator aware this vms worker already has the vms identifier
        this.coordinatorQueue.add(this.consumerVms);
    }

    private void sendBatchCommitInfo(BatchCommitInfo.Payload batchCommitInfo){
        // then send only the batch commit info
        LOGGER.log(DEBUG, "Leader: Batch ("+batchCommitInfo.batch()+") commit info will be sent to " + this.consumerVms.identifier);
        try {
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            BatchCommitInfo.write(writeBuffer, batchCommitInfo);
            writeBuffer.flip();
            this.WRITE_SYNCHRONIZER.take();
            this.channel.write(writeBuffer, this.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
        } catch (Exception e) {
            this.pendingMessages.add(batchCommitInfo);
            if(this.WRITE_SYNCHRONIZER.isEmpty()){
                this.WRITE_SYNCHRONIZER.add(DUMB);
            }
            return;
        }
        LOGGER.log(INFO, "Leader: Batch ("+batchCommitInfo.batch()+") commit info sent to " + this.consumerVms.identifier+"\n"+batchCommitInfo);
    }

    private ByteBuffer retrieveByteBuffer() {
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) return bb;
        //logger.log(INFO, "New ByteBuffer will be created");
        // leads to several bugs =(
        // return ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        return MemoryManager.getTemporaryDirectBuffer(this.networkBufferSize);
    }

    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        this.writeBufferPool.add(bb);
    }

    private final List<TransactionEvent.PayloadRaw> transactionEvents = new ArrayList<>(1024);

    /**
     * While a write operation is in progress, it must wait for completion and then submit the next write.
     */
    private void sendBatchOfEvents(){
        int remaining = this.transactionEvents.size();
        int count = remaining;
        ByteBuffer writeBuffer;
        while(remaining > 0) {
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, this.transactionEvents, writeBuffer);

                // without this async handler, the bytebuffer arriving in the VMS can be corrupted
                // relying on future.get() yields corrupted buffer in the consumer
                LOGGER.log(INFO, "Leader: Submitting ["+(count - remaining)+"] events to "+consumerVms.identifier);
                count = remaining;

                writeBuffer.flip();

                this.WRITE_SYNCHRONIZER.take();
                this.channel.write(writeBuffer, networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.batchWriteCompletionHandler);

            } catch (Exception e) {
                LOGGER.log(ERROR, "Leader: Error on submitting ["+count+"] events to "+this.consumerVms.identifier+":"+e);
                // return events to the deque
                while(!this.transactionEvents.isEmpty()) {
                    if(this.transactionEventQueue.offer(this.transactionEvents.get(0))){
                        this.transactionEvents.remove(0);
                    }
                }

                // force exit loop
                remaining = 0;

                if(this.WRITE_SYNCHRONIZER.isEmpty()){
                    this.WRITE_SYNCHRONIZER.add(DUMB);
                }

            }
        }
        this.transactionEvents.clear();
    }

    private final Deque<ByteBuffer> PENDING_WRITES_BUFFER = new ConcurrentLinkedDeque<>();

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    /**
     * For commit-related messages
     */
    private final class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            if(byteBuffer.hasRemaining()) {
                LOGGER.log(WARNING, "Leader: Found not all bytes of message (type: "+byteBuffer.get(0)+") were sent to "+consumerVms.identifier+" Trying to send the remaining now...");
                channel.write(byteBuffer, networkSendTimeout, TimeUnit.MILLISECONDS, byteBuffer, this);
                return;
            }
            WRITE_SYNCHRONIZER.add(DUMB);
            returnByteBuffer(byteBuffer);
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            WRITE_SYNCHRONIZER.add(DUMB);
            LOGGER.log(ERROR, "Leader: ERROR on writing batch of events to "+consumerVms.identifier+": "+exc);
            returnByteBuffer(byteBuffer);
        }
    }

    private final BatchWriteCompletionHandler batchWriteCompletionHandler = new BatchWriteCompletionHandler();

    private final class BatchWriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            LOGGER.log(DEBUG, "Leader: Message with size " + result + " has been sent to: " + consumerVms.identifier);
            if(byteBuffer.hasRemaining()) {
                // keep the lock and send the remaining
                channel.write(byteBuffer, networkSendTimeout, TimeUnit.MILLISECONDS, byteBuffer, this);
            } else {
                WRITE_SYNCHRONIZER.add(DUMB);
                returnByteBuffer(byteBuffer);
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            WRITE_SYNCHRONIZER.add(DUMB);
            LOGGER.log(ERROR, "Leader: ERROR on writing batch of events to "+consumerVms.identifier+": "+exc);
            byteBuffer.position(0);
            PENDING_WRITES_BUFFER.offer(byteBuffer);
            LOGGER.log(INFO,  "Leader: Byte buffer added to pending queue. #pending: "+PENDING_WRITES_BUFFER.size());
        }
    }

    enum State {
        NEW,
        CONNECTION_ESTABLISHED,
        CONNECTION_FAILED,
        LEADER_PRESENTATION_SENT,
        LEADER_PRESENTATION_SEND_FAILED,
        VMS_PRESENTATION_RECEIVED,
        VMS_PRESENTATION_RECEIVE_FAILED,
        VMS_PRESENTATION_PROCESSED,
        CONSUMER_SET_READY_FOR_SENDING,
        CONSUMER_SET_SENDING_FAILED,
        CONSUMER_EXECUTING
    }

}
