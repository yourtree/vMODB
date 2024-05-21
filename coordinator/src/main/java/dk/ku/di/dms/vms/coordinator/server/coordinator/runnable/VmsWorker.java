package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.IVmsWorker.State.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static java.lang.System.Logger.Level.*;
import static java.lang.Thread.sleep;

@SuppressWarnings("SequencedCollectionMethodCanBeUsed")
final class VmsWorker extends StoppableRunnable implements IVmsWorker {

    private static final System.Logger logger = System.getLogger(VmsWorker.class.getName());
    
    private final ServerNode me;

    // the vms this worker is responsible for
    private final IdentifiableNode consumerVms;

    private final int networkBufferSize;

    private final int networkSendTimeout;

    // defined after presentation being sent by the actual vms
    private VmsNode vmsNode = new VmsNode("", 0, "unknown", 0, 0, 0, null, null, null);

    private State state;

    private final IVmsSerdesProxy serdesProxy;

    private final ByteBuffer readBuffer;

    private final BlockingDeque<ByteBuffer> writeBufferPool;

    /**
     * Queues to inform coordinator about an event
     */
    private final BlockingQueue<Coordinator.Message> coordinatorQueue;

    private final AsynchronousChannelGroup group;

    private AsynchronousSocketChannel channel;

    // DTs particular to this vms worker
    private final BlockingDeque<TransactionEvent.PayloadRaw> transactionEvents;

    private final BlockingQueue<Message> messageQueue;
    
    private final BlockingQueue<Byte> WRITE_SYNCHRONIZER = new ArrayBlockingQueue<>(1);

    private static final Byte DUMB = 1;

    static VmsWorker buildAsStarter(// coordinator reference
                                    ServerNode me,
                                    // the vms this thread is responsible for
                                    IdentifiableNode consumerVms,
                                    // shared data structure to communicate messages to coordinator
                                    BlockingQueue<Coordinator.Message> coordinatorQueue,
                                    // the group for the socket channel
                                    AsynchronousChannelGroup group,
                                    int networkBufferSize,
                                    int networkSendTimeout,
                                    IVmsSerdesProxy serdesProxy) {
        // passing null is clearly not a good option. when it comes to support VMS node crashes, it will be necessary to rethink this design
        return new VmsWorker(me, consumerVms, coordinatorQueue, null, group, MemoryManager.getTemporaryDirectBuffer(networkBufferSize), networkBufferSize, networkSendTimeout, serdesProxy);
    }

    static VmsWorker buildAsUnknown(
            ServerNode me,
            NetworkAddress consumerVms,
            BlockingQueue<Coordinator.Message> coordinatorQueue,
            // the socket channel already established
            AsynchronousSocketChannel channel,
            AsynchronousChannelGroup group,
            // to continue reading presentation
            ByteBuffer readBuffer,
            int networkBufferSize,
            int networkSendTimeout,
            IVmsSerdesProxy serdesProxy) {
        return new VmsWorker(me, new IdentifiableNode("unknown", consumerVms.host, consumerVms.port), coordinatorQueue, channel, group, readBuffer, networkBufferSize, networkSendTimeout, serdesProxy);
    }

    private VmsWorker(// coordinator reference
                      ServerNode me,
                      // the vms this thread is responsible for
                      IdentifiableNode consumerVms,
                      // events to share with coordinator
                      BlockingQueue<Coordinator.Message> coordinatorQueue,
                      // the group for socket channel
                      AsynchronousSocketChannel channel,
                      AsynchronousChannelGroup group,
                      ByteBuffer readBuffer,
                      int networkBufferSize,
                      int networkSendTimeout,
                      IVmsSerdesProxy serdesProxy) {
        this.me = me;
        this.state = State.NEW;
        this.consumerVms = consumerVms;

        this.channel = channel;
        this.group = group;

        this.networkBufferSize = networkBufferSize;
        this.networkSendTimeout = networkSendTimeout;

        // initialize the write buffer
        this.writeBufferPool = new LinkedBlockingDeque<>();

        this.readBuffer = readBuffer;
        this.serdesProxy = serdesProxy;

        // to allow the first thread to write
        this.WRITE_SYNCHRONIZER.add(DUMB);

        // in
        this.transactionEvents = new LinkedBlockingDeque<>();
        this.messageQueue = new LinkedBlockingQueue<>();

        // out - shared by many vms workers
        this.coordinatorQueue = coordinatorQueue;
    }

    public void initHandshakeProtocol(){
        // a vms has tried to connect
        if(this.channel != null) {
            this.state = CONNECTION_ESTABLISHED;
            this.processVmsIdentifier();
            this.readBuffer.clear();
            this.channel.read( this.readBuffer, null, new VmsReadCompletionHandler() );
            return;
        }

        // connect to starter vms
        logger.log(INFO, "Leader: Attempting connection to "+this.consumerVms.identifier);
        try {
            this.channel = AsynchronousSocketChannel.open(this.group);
            NetworkUtils.configure(this.channel, networkBufferSize);

            while(true) {
                try {
                    this.channel.connect(this.consumerVms.asInetSocketAddress()).get();
                    break;
                } catch (Exception e) {
                    logger.log(ERROR, "Leader: Error on connecting to " + consumerVms.identifier + ": " + e.getMessage()+"\n Trying connecting again in 1s...");
                    sleep(1000);
                }
            }
            this.state = CONNECTION_ESTABLISHED;

            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            try {
                // write presentation
                Presentation.writeServer(writeBuffer, this.me, true);
                writeBuffer.flip();
                this.WRITE_SYNCHRONIZER.take();
                this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
            } catch(Exception e){
                logger.log(ERROR, "Error on writing presentation: "+e.getCause().toString());
                if(this.WRITE_SYNCHRONIZER.isEmpty()){
                    this.WRITE_SYNCHRONIZER.add(DUMB);
                }
                return;
            }

            this.state = State.LEADER_PRESENTATION_SENT;

            // set read handler here
            this.channel.read( this.readBuffer, null, new VmsReadCompletionHandler() );

        } catch (Exception e) {
            logger.log(WARNING,"Failed to connect to a known VMS: " + this.consumerVms.identifier);
            if (this.state == State.NEW) {
                // forget about it, let the vms connect then...
                this.state = State.CONNECTION_FAILED;
            } else if(this.state == CONNECTION_ESTABLISHED) {
                this.state = LEADER_PRESENTATION_SEND_FAILED;
                // check if connection is still online. if so, try again
                // otherwise, retry connection in a few minutes
                if(this.channel.isOpen()){
                    // try again? what is he problem?
                    logger.log(WARNING,"It was not possible to send a presentation message, although the channel is open. The connection will be closed now.");
                    try { this.channel.close(); } catch (IOException ignored) { }
                } else {
                    logger.log(WARNING,"It was not possible to send a presentation message and the channel is not open. Check the consumer VMS: " + consumerVms);
                }
            } else {
                logger.log(WARNING,"Cannot find the root problem. Please have a look: "+e.getCause().getMessage());
            }

            // important for consistency of state (if debugging, good to see the code controls the thread state)
            this.stop();
        }
    }

    @Override
    public void run() {
        // initialize buffer pool
        this.writeBufferPool.addFirst( retrieveByteBuffer() );
        this.writeBufferPool.addFirst( retrieveByteBuffer() );

        this.initHandshakeProtocol();
        this.eventLoop();
    }

    private final List<TransactionEvent.PayloadRaw> localTxEvents = new ArrayList<>(1024);

    private static final int MAX_SLEEP = 1000;

    /**
     * Event loop must not be blocking because it performs two tasks:
     * (a) get and process batch-tracking messages from coordinator
     * (b) process transaction input events
     */
    private void eventLoop() {
        int pollTimeout = 50;
        Message message;
        while (this.isRunning()){
            try {
                message = this.messageQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
                if(message == null){
                    if(this.transactionEvents.isEmpty()){
                        pollTimeout = Math.min(pollTimeout * 2, MAX_SLEEP);
                    } else {
                        this.sendBatchedEvents();
                    }
                } else {
                    pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
                    switch (message.type()) {
                        // in order of probability
                        case SEND_BATCH_COMMIT_INFO -> this.sendBatchCommitInfo(message.asBatchCommitInfo());
                        case SEND_BATCH_COMMIT_COMMAND -> this.sendBatchCommitRequest(message);
                        case SEND_TRANSACTION_ABORT -> this.sendTransactionAbort(message);
                        case SEND_CONSUMER_SET -> this.sendConsumerSet(message);
                    }

                    if(!this.transactionEvents.isEmpty()){
                        this.sendBatchedEvents();
                    }

                }

                this.processPendingWrites();

            } catch (InterruptedException e) {
                logger.log(ERROR, "Leader: VMS worker for "+this.vmsNode.identifier+" has been interrupted: "+e);
                this.stop();
            } catch (Exception e) {
                logger.log(ERROR, "Leader: VMS worker for "+this.vmsNode.identifier+" has caught an exception: "+e);
            }
        }
    }

    private void processPendingWrites() {
        ByteBuffer bb = this.PENDING_WRITES_BUFFER.poll();
        if(bb != null){
            logger.log(INFO, "Leader: Retrying sending failed buffer send");
            try {
                this.WRITE_SYNCHRONIZER.take();
                this.channel.write(bb, networkSendTimeout, TimeUnit.MILLISECONDS, bb, this.batchWriteCompletionHandler);
            } catch (Exception e){
                logger.log(ERROR, "Leader: ERROR on retrying to send failed buffer: \n"+e);
                if(e instanceof IllegalStateException){
                    // probably comes from the class {@AsynchronousSocketChannelImpl}:
                    // "Writing not allowed due to timeout or cancellation"
                    try { sleep(100); } catch (InterruptedException ignored) { }
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

    @Override
    public void queueEvent(TransactionEvent.PayloadRaw payload){
        boolean sent = this.transactionEvents.offer(payload);
        while (!sent){
            sent = this.transactionEvents.offer(payload);
        }
    }

    @Override
    public void queueMessage(Message message) {
        boolean sent = this.messageQueue.offer(message);
        while(!sent) {
            sent = this.messageQueue.offer(message);
        }
    }

    private void sendTransactionAbort(Message workerMessage) {
        TransactionAbort.Payload tidToAbort = workerMessage.asTransactionAbort();
        try {
            ByteBuffer writeBuffer = retrieveByteBuffer();
            TransactionAbort.write(writeBuffer, tidToAbort);
            writeBuffer.flip();
            this.WRITE_SYNCHRONIZER.take();
            this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
            logger.log(WARNING,"Leader: Transaction abort sent to: " + this.vmsNode.identifier);
        } catch (InterruptedException e){
            if(channel.isOpen()){
                logger.log(WARNING,"Leader: Transaction abort write has failed but channel is open. Trying to write again to: "+consumerVms.identifier+" in a while");
                this.messageQueue.add(workerMessage);
            } else {
                logger.log(WARNING,"Leader: Transaction abort write has failed and channel is closed: "+this.consumerVms.identifier);
                this.stop(); // no reason to continue the loop
            }
        }
    }

    private void sendBatchCommitRequest(Message workerMessage) {
        BatchCommitCommand.Payload commitRequest = workerMessage.asBatchCommitCommand();
        try {
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            BatchCommitCommand.write(writeBuffer, commitRequest);
            writeBuffer.flip();

            this.WRITE_SYNCHRONIZER.take();
            this.channel.write(writeBuffer, networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            logger.log(INFO, "Leader: Commit request sent to: " + this.vmsNode.identifier);
        } catch (Exception e){
            if(this.channel.isOpen()){
                logger.log(WARNING,"Commit request write has failed but channel is open. Trying to write again to: "+this.vmsNode.identifier+" in a while");
                this.messageQueue.add(workerMessage);
            } else {
                logger.log(WARNING,"Commit request write has failed and channel is closed: "+this.vmsNode.identifier);
                this.stop(); // no reason to continue the loop
            }

            if(this.WRITE_SYNCHRONIZER.isEmpty()){
                this.WRITE_SYNCHRONIZER.add(DUMB);
            }
        }
    }

    private void sendConsumerSet(Message workerMessage) {
        // the first or new information
        if(this.state == VMS_PRESENTATION_PROCESSED) {
            this.state = CONSUMER_SET_READY_FOR_SENDING;
            logger.log(INFO, "Leader: Consumer set will be sent to: "+this.consumerVms.identifier);
        } else if(this.state == CONSUMER_EXECUTING){
            logger.log(INFO, "Leader: Consumer set is going to be updated for: "+this.consumerVms.identifier);
        } else if(this.state == CONSUMER_SET_SENDING_FAILED){
            logger.log(INFO, "Leader: Consumer set, another attempt to write to: "+this.consumerVms.identifier);
        } // else, nothing...

        String vmsConsumerSet = workerMessage.asVmsConsumerSet();
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
            if (channel.isOpen()) {
                logger.log(WARNING,"Write has failed but channel is open. Trying to write again to: " + consumerVms + " in a while");
                // just queue again
                this.messageQueue.add(workerMessage);
            } else {
                logger.log(WARNING,"Write has failed and channel is closed: " + consumerVms);
                this.stop(); // no reason to continue the loop
            }
        } catch (IOException | BufferOverflowException e){
            this.state = CONSUMER_SET_SENDING_FAILED;
            logger.log(WARNING,"Write has failed and the VMS worker will undergo an unknown state: " + consumerVms);
        }

    }

    /**
     * Reuses the thread from the socket thread pool, instead of assigning a specific thread
     * Removes thread context switching costs.
     * This thread should not block.
     * The idea is to decode the message and deliver back to socket loop as soon as possible
     * This thread must be set free as soon as possible, should not do long-running computation
     */
    private final class VmsReadCompletionHandler implements CompletionHandler<Integer, Void> {

        // is it an abort, a commit response?
        // it cannot be replication because have opened another channel for that

        @SuppressWarnings("UnnecessaryLocalVariable")
        @Override
        public void completed(Integer result, Void connectionMetadata) {

            if(result == -1){
                logger.log(INFO, "Leader: " + vmsNode.identifier+" has disconnected");
                return;
            }

            // decode message by getting the first byte
            byte type = readBuffer.get(0);
            readBuffer.position(1);

            switch (type) {
                case PRESENTATION -> {
                    // this is a very bad conditional statement
                    // we could do better removing this concept of "unknown" and simply check the state
                    if(state == LEADER_PRESENTATION_SENT){
                        state = VMS_PRESENTATION_RECEIVED;// for the first time
                        processVmsIdentifier();
                        state = VMS_PRESENTATION_PROCESSED;
                    } else {
                        // in the future it can be an update of the vms schema or crash recovery
                        logger.log(ERROR, "Leader: Presentation already received from VMS: "+vmsNode.identifier);
                    }
                }
                // from all terminal VMSs involved in the last batch
                case BATCH_COMPLETE -> {
                    // don't actually need the host and port in the payload since we have the attachment to this read operation...
                    BatchComplete.Payload response = BatchComplete.read(readBuffer);
                    logger.log(INFO, "Leader: Batch ("+response.batch()+") complete received from: "+vmsNode.identifier);
                    // must have a context, i.e., what batch, the last?
                    coordinatorQueue.add( new Coordinator.Message( Coordinator.Type.BATCH_COMPLETE, response));
                    // if one abort, no need to keep receiving
                    // actually it is unclear in which circumstances a vms would respond no... probably in case it has not received an ack from an aborted commit response?
                    // because only the aborted transaction will be rolled back
                }
                case BATCH_COMMIT_ACK -> {
                    logger.log(INFO, "Leader: Batch commit ACK received from: "+vmsNode.identifier);
                    BatchCommitAck.Payload response = BatchCommitAck.read(readBuffer);
                    // logger.config("Just logging it, since we don't necessarily need to wait for that. "+response);
                    coordinatorQueue.add( new Coordinator.Message( Coordinator.Type.BATCH_COMMIT_ACK, response));
                }
                case TX_ABORT -> {
                    // get information of what
                    TransactionAbort.Payload response = TransactionAbort.read(readBuffer);
                    coordinatorQueue.add( new Coordinator.Message( Coordinator.Type.TRANSACTION_ABORT, response));
                }
                case EVENT ->
                        logger.log(INFO, "Leader: New event received from: "+vmsNode.identifier);
                case BATCH_OF_EVENTS -> //
                        logger.log(INFO, "Leader: New batch of events received from VMS");
                default ->
                        logger.log(WARNING, "Leader: Unknown message received.");

            }

            // must take into account more data has arrived. otherwise can lead to losing a batch complete
            if(readBuffer.position() < carryOn + result){
                int aux = carryOn + result;
                carryOn = aux;
            } else {
                carryOn = 0;
                readBuffer.clear();
            }
            channel.read( readBuffer, null, this );
        }

        private volatile int carryOn = 0;

        @Override
        public void failed(Throwable exc, Void attachment) {
            if(state == LEADER_PRESENTATION_SENT){
                state = VMS_PRESENTATION_RECEIVE_FAILED;
                logger.log(WARNING,"It was not possible to receive a presentation message from consumer VMS: "+exc.getMessage());
            } else {
                if (channel.isOpen()) {
                    logger.log(WARNING,"Read has failed but channel is open. Trying to read again from: " + consumerVms);

                } else {
                    logger.log(WARNING,"Read has failed and channel is closed: " + consumerVms);
                }
            }
            readBuffer.clear();
            channel.read(readBuffer, null, this);
        }
    }

    private void processVmsIdentifier() {
        // always a vms
        this.readBuffer.position(2);
        this.vmsNode = Presentation.readVms(readBuffer, serdesProxy);
        this.state = State.VMS_PRESENTATION_PROCESSED;
        // let coordinator aware this vms worker already has the vms identifier
        this.coordinatorQueue.add(new Coordinator.Message(Coordinator.Type.VMS_IDENTIFIER, new VmsIdentifier(this.vmsNode, this)));
    }

    private void sendBatchCommitInfo(BatchCommitInfo.Payload batchCommitInfo){
        // then send only the batch commit info
        logger.log(INFO, "Leader: Batch ("+batchCommitInfo.batch()+") commit info will be sent to: " + this.vmsNode.identifier);
        try {
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            BatchCommitInfo.write(writeBuffer, batchCommitInfo);
            writeBuffer.flip();
            this.WRITE_SYNCHRONIZER.take();
            this.channel.write(writeBuffer, networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
        } catch (Exception e) {
            if(!this.channel.isOpen()) {
                this.vmsNode.off();
            }
            if(this.WRITE_SYNCHRONIZER.isEmpty()){
                this.WRITE_SYNCHRONIZER.add(DUMB);
            }
        }
        logger.log(INFO, "Leader: Batch ("+batchCommitInfo.batch()+") commit info sent to: " + this.vmsNode.identifier);
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

    /**
     * While a write operation is in progress, it must wait for completion and then submit the next write.
     */
    private void sendBatchedEvents(){
        this.transactionEvents.drainTo(this.localTxEvents);
        int remaining = this.localTxEvents.size();
        int count = remaining;
        ByteBuffer writeBuffer;
        while(remaining > 0) {
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, this.localTxEvents, writeBuffer);

                // without this async handler, the bytebuffer arriving in the VMS can be corrupted
                // relying on future.get() yields corrupted buffer in the consumer
                logger.log(INFO, "Leader: Submitting ["+(count - remaining)+"] events to "+vmsNode.identifier);
                count = remaining;

                //writeBuffer.flip();
                writeBuffer.position(0);

                this.WRITE_SYNCHRONIZER.take();
                this.channel.write(writeBuffer, networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.batchWriteCompletionHandler);

            } catch (Exception e) {
                logger.log(ERROR, "Leader: Error on submitting ["+count+"] events to "+this.vmsNode.identifier+":"+e);
                // return events to the deque
                while(!this.localTxEvents.isEmpty()) {
                    if(this.transactionEvents.offerFirst(this.localTxEvents.get(0))){
                        this.localTxEvents.remove(0);
                    }
                }

                // force exit loop
                remaining = 0;

                if(this.WRITE_SYNCHRONIZER.isEmpty()){
                    this.WRITE_SYNCHRONIZER.add(DUMB);
                }

            }
        }
        this.localTxEvents.clear();
    }

    private final BlockingDeque<ByteBuffer> PENDING_WRITES_BUFFER = new LinkedBlockingDeque<>();

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    /**
     * For commit-related messages
     */
    private final class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            if(byteBuffer.hasRemaining()) {
                logger.log(ERROR, "Leader: Found not all bytes of commit message were sent to "+consumerVms.identifier);
            }
            WRITE_SYNCHRONIZER.add(DUMB);
        }

        @Override
        public void failed(Throwable exc, ByteBuffer attachment) {
            WRITE_SYNCHRONIZER.add(DUMB);
            logger.log(ERROR, "Leader: ERROR on writing batch of events to "+consumerVms.identifier+": "+exc);
        }
    }

    private final BatchWriteCompletionHandler batchWriteCompletionHandler = new BatchWriteCompletionHandler();

    private final class BatchWriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            logger.log(DEBUG, "Leader: Message with size " + result + " has been sent to: " + consumerVms.identifier);
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
            logger.log(ERROR, "Leader: ERROR on writing batch of events to "+consumerVms.identifier+": "+exc);
            byteBuffer.clear();
            boolean sent = PENDING_WRITES_BUFFER.offer(byteBuffer);
            while(!sent){
                sent = PENDING_WRITES_BUFFER.offer(byteBuffer);
            }
            logger.log(INFO,  "Leader: Byte buffer added to pending queue. #pending: "+PENDING_WRITES_BUFFER.size());
        }
    }

}
