package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitResponse;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionResult;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.ingest.BulkDataLoader;
import dk.ku.di.dms.vms.sdk.embed.scheduler.BatchContext;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.Issue;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.*;
import static dk.ku.di.dms.vms.web_common.meta.Issue.Category.CANNOT_CONNECT_TO_NODE;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * This default event handler connects direct to the coordinator
 * So in this approach it bypasses the sidecar. In this way,
 * the DBMS must also be run within this code.
 *
 * The virtual microservice don't know who is the coordinator. It should be passive.
 * The leader and followers must share a list of VMSs.
 * Could also try to adapt to JNI:
 * <a href="https://nachtimwald.com/2017/06/17/calling-java-from-c/">...</a>
 */
public final class EmbedVmsEventHandler extends SignalingStoppableRunnable {

    /** EXECUTOR SERVICE (for socket channels) **/
    private ExecutorService taskExecutor;

    /** SERVER SOCKET **/
    // other VMSs may want to connect in order to send events
    private final AsynchronousServerSocketChannel serverSocket;

    private AsynchronousChannelGroup group;

    /** INTERNAL CHANNELS **/
    private final VmsEmbedInternalChannels vmsInternalChannels;

    /** VMS METADATA **/
    private final VmsIdentifier me; // this merges network and semantic data about the vms
    private final VmsRuntimeMetadata vmsMetadata;

    /** EXTERNAL VMSs **/
    private final Map<String, NetworkNode> consumerVms; // sent by coordinator
    private final Map<Integer, ConnectionMetadata> vmsConnectionMetadataMap;

    /** SERIALIZATION **/
    private final IVmsSerdesProxy serdesProxy;

    /** COORDINATOR **/
    private ServerIdentifier leader;
    private ConnectionMetadata leaderConnectionMetadata;

    /** INTERNAL STATE **/
    private BatchContext currentBatch;

    // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    public EmbedVmsEventHandler(VmsEmbedInternalChannels vmsInternalChannels, // for communicating with other components
                                VmsIdentifier me, // to identify which vms this is
                                VmsRuntimeMetadata vmsMetadata, // metadata about this vms
                                IVmsSerdesProxy serdesProxy, // ser/des of objects
                                ExecutorService executorService // for recurrent and continuous tasks
    ) throws IOException {
        super();

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;

        this.vmsMetadata = vmsMetadata;
        this.vmsConnectionMetadataMap = new ConcurrentHashMap<>(10);
        this.consumerVms = new ConcurrentHashMap<>(10);

        this.serdesProxy = serdesProxy;
        this.taskExecutor = executorService;

        this.group = AsynchronousChannelGroup.withThreadPool(executorService);
        this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        SocketAddress address = new InetSocketAddress(me.host, me.port);
        serverSocket.bind(address);

        this.currentBatch = new BatchContext(me.lastBatch, me.lastTid);
        this.currentBatch.setStatus(BatchContext.Status.COMPLETION_INFORMED);
        this.batchContextMap = new ConcurrentHashMap<>(3);

    }

    public EmbedVmsEventHandler(VmsEmbedInternalChannels vmsInternalChannels, // for communicating with other components
                                VmsIdentifier me, // to identify which vms this is
                                VmsRuntimeMetadata vmsMetadata, // metadata about this vms
                                IVmsSerdesProxy serdesProxy // ser/des of objects
    ) throws IOException {
        super();

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;

        this.vmsMetadata = vmsMetadata;
        this.vmsConnectionMetadataMap = new ConcurrentHashMap<>(10);
        this.consumerVms = new ConcurrentHashMap<>(10);

        this.serdesProxy = serdesProxy;
        this.serverSocket = AsynchronousServerSocketChannel.open(null);
        SocketAddress address = new InetSocketAddress(me.host, me.port);
        serverSocket.bind(address);

        this.currentBatch = new BatchContext(me.lastBatch, me.lastTid);
        this.currentBatch.setStatus(BatchContext.Status.COMPLETION_INFORMED);
        this.batchContextMap = new ConcurrentHashMap<>(3);

    }

    /**
     * A thread that basically writes events to other VMSs and the Leader
     * Retrieves data from all output queues
     *
     * All output queues must be read in order to send their data
     *
     * A batch strategy for sending would involve sleeping until the next timeout for batch,
     * send and set up the next. Do that iteratively
     */
    @Override
    public void run() {

        logger.info("Event handler has started running.");

        // setup accept since we need to accept connections from the coordinator and other VMSs
        serverSocket.accept( null, new AcceptCompletionHandler());

        // must wait for the consumer set before starting receiving transactions
        // this is guaranteed by the coordinator, the one who is sending transaction requests

        logger.info("Accept handler has been setup.");

        // while not stopped
        while(isRunning()){

            //  write events to leader and VMSs...
            try {

//                if(!vmsInternalChannels.transactionAbortOutputQueue().isEmpty()){
//                    // TODO handle
//                }

                // it is better to get all the results of a given transaction instead of one by one. it must be atomic anyway
                if(!vmsInternalChannels.transactionOutputQueue().isEmpty()){

                    VmsTransactionResult txResult = vmsInternalChannels.transactionOutputQueue().take();

                    // handle
                    logger.info("New transaction result in event handler. TID = "+txResult.tid);

                    if(this.currentBatch.status() == BatchContext.Status.NEW && this.currentBatch.lastTid == txResult.tid){
                        // we need to alert the scheduler...
                        logger.info("The last TID for the current batch has arrived. Time to inform the coordinator about the completion.");
                        // this.vmsInternalChannels.batchContextQueue().add( this.currentBatch );

                        // many outputs from the same transaction may arrive here, but can only send the batch commit once
                        this.currentBatch.setStatus(BatchContext.Status.COMPLETED);

                        informBatchCompletion();
                    }

                    // what could go wrong in terms of interleaving? what if this tid is the last of a given batch?
                    // may not happen because the batch commit info is processed before events are sent to the scheduler
                    // to remove possibility of interleaving completely, it is better to call

                    // just send events to appropriate targets
                    for(OutboundEventResult outputEvent : txResult.resultTasks){
                        if(!outputEvent.terminal()){
                            bufferEventToVms(outputEvent);
                        }
                    }

                    moveBatchIfNecessary();

                }

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Problem on handling event on event handler:"+e.getMessage());
            }

        }

        logger.info("Event handler has finished execution.");

    }

    /**
     * TODO can pass a completion handler to the scheduler, so no need to have this on the event loop
     */
    private void moveBatchIfNecessary() {
        if(this.currentBatch.status() == BatchContext.Status.COMMITTED){

            this.currentBatch.setStatus(BatchContext.Status.REPLYING_COMMITTED);

            this.leaderConnectionMetadata.writeLock.acquireUninterruptibly();
            this.leaderConnectionMetadata.writeBuffer.clear();
            BatchCommitResponse.write( leaderConnectionMetadata.writeBuffer, this.currentBatch.batch, this.me );
            this.leaderConnectionMetadata.writeBuffer.flip();

            this.leaderConnectionMetadata.channel.write(this.leaderConnectionMetadata.writeBuffer, null,
                    new CompletionHandler<>() {

                        @Override
                        public void completed(Integer result, Object attachment) {
                            currentBatch.setStatus(BatchContext.Status.COMMIT_INFORMED);

                            // do I have the next batch already?
                            var newBatch = batchContextMap.get( currentBatch.batch + 1 );
                            if(newBatch != null){
                                currentBatch = newBatch;
                            } // otherwise must still receive it

                            leaderConnectionMetadata.writeBuffer.clear();
                            leaderConnectionMetadata.writeLock.release();
                        }

                        @Override
                        public void failed(Throwable exc, Object attachment) {
                            leaderConnectionMetadata.writeBuffer.clear();
                            if(!leaderConnectionMetadata.channel.isOpen()){
                                leader.off();
                            }
                            leaderConnectionMetadata.writeLock.release();
                        }
                    }
            );

        }
    }

    private void informBatchCompletion() {

        this.currentBatch.setStatus(BatchContext.Status.REPLYING_COMPLETED);

        this.leaderConnectionMetadata.writeLock.acquireUninterruptibly();
        this.leaderConnectionMetadata.writeBuffer.clear();
        BatchComplete.write( leaderConnectionMetadata.writeBuffer, this.currentBatch.batch, this.me );
        this.leaderConnectionMetadata.writeBuffer.flip();

        this.leaderConnectionMetadata.channel.write(this.leaderConnectionMetadata.writeBuffer, null,
                new CompletionHandler<>() {

                    @Override
                    public void completed(Integer result, Object attachment) {
                        currentBatch.setStatus(BatchContext.Status.COMPLETION_INFORMED);
                        leaderConnectionMetadata.writeBuffer.clear();
                        leaderConnectionMetadata.writeLock.release();
                    }

                    @Override
                    public void failed(Throwable exc, Object attachment) {
                        leaderConnectionMetadata.writeBuffer.clear();
                        if(!leaderConnectionMetadata.channel.isOpen()){
                            leader.off();
                        }
                        leaderConnectionMetadata.writeLock.release();
                    }
                }
        );

    }

    private void processNewBatchInfo(BatchCommitInfo.Payload batchCommitInfo){

        BatchContext batchContext = new BatchContext(batchCommitInfo.batch(), batchCommitInfo.tid());
        this.batchContextMap.put(batchCommitInfo.batch(), batchContext);
        if(this.currentBatch.status() == BatchContext.Status.COMPLETION_INFORMED && batchCommitInfo.batch() == currentBatch.batch + 1){
            this.currentBatch = batchContext;
        }

    }

    private void bufferEventToVms(OutboundEventResult outputEvent){

        NetworkNode vms = consumerVms.get(outputEvent.outputQueue());

        if(vms == null){
            logger.warning(
                    "An output event (queue: "+outputEvent.outputQueue()+") has no target virtual microservice.");
            return;
        }

        logger.info("An output event (queue: "+outputEvent.outputQueue()+") will be sent to vms: "+vms);

        ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get(vms.hashCode());

        connectionMetadata.writeLock.acquireUninterruptibly();

        Class<?> clazz = vmsMetadata.queueToEventMap().get(outputEvent.outputQueue());

        String objStr = serdesProxy.serialize(outputEvent.output(), clazz);

        TransactionEvent.write( connectionMetadata.writeBuffer, outputEvent.tid(), outputEvent.lastTid(), outputEvent.batch(), outputEvent.outputQueue(), objStr );

        // TODO must be sent in a batch appropriately

        connectionMetadata.writeBuffer.flip();

        connectionMetadata.channel.write(connectionMetadata.writeBuffer, connectionMetadata,
                new CompletionHandler<>() {

                    @Override
                    public void completed(Integer result, ConnectionMetadata connectionMetadata) {
                        connectionMetadata.writeBuffer.clear();
                        connectionMetadata.writeLock.release();
                    }

                    @Override
                    public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
                        connectionMetadata.writeBuffer.clear();
                        connectionMetadata.writeLock.release();
                    }
                }
        );

    }

    private void sendEventToLeader(OutboundEventResult outputEvent) throws InterruptedException {
        leaderConnectionMetadata.writeLock.acquire();
        leaderConnectionMetadata.writeBuffer.clear();

        Class<?> clazz = vmsMetadata.queueToEventMap().get(outputEvent.outputQueue());

        String objStr = serdesProxy.serialize(outputEvent.output(), clazz);

        TransactionEvent.write( leaderConnectionMetadata.writeBuffer, outputEvent.tid(), 0, 0, outputEvent.outputQueue(), objStr );
        leaderConnectionMetadata.writeBuffer.flip();

        leaderConnectionMetadata.channel.write(leaderConnectionMetadata.writeBuffer, null,
                new CompletionHandler<>() {

                    @Override
                    public void completed(Integer result, Object attachment) {
                        leaderConnectionMetadata.writeBuffer.clear();
                        leaderConnectionMetadata.writeLock.release();
                    }

                    @Override
                    public void failed(Throwable exc, Object attachment) {
                        leaderConnectionMetadata.writeBuffer.clear();
                        if(!leaderConnectionMetadata.channel.isOpen()){
                            leader.off();
                        }
                        leaderConnectionMetadata.writeLock.release();
                    }
                }
        );
    }

    private class ConnectToExternalVmsProtocol {

        private State state;
        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;
        public final CompletionHandler<Void, ConnectToExternalVmsProtocol> connectCompletionHandler;
        private final NetworkNode node;

        public ConnectToExternalVmsProtocol(AsynchronousSocketChannel channel, NetworkNode node) {
            this.state = State.NEW;
            this.channel = channel;
            this.connectCompletionHandler = new ConnectToVmsCH();
            this.buffer = MemoryManager.getTemporaryDirectBuffer(1024);
            this.node = node;
        }

        private enum State {
            NEW,
            CONNECTED,
            PRESENTATION_SENT
        }

        private class ConnectToVmsCH implements CompletionHandler<Void, ConnectToExternalVmsProtocol> {

            @Override
            public void completed(Void result, ConnectToExternalVmsProtocol attachment) {

                attachment.state = State.CONNECTED;

                ConnectionMetadata connMetadata = new ConnectionMetadata(
                        node.hashCode(),
                        ConnectionMetadata.NodeType.VMS,
                        MemoryManager.getTemporaryDirectBuffer(),
                        MemoryManager.getTemporaryDirectBuffer(),
                        channel,
                        new Semaphore(1));

                vmsConnectionMetadataMap.put(node.hashCode(), connMetadata);

                String dataSchema = serdesProxy.serializeDataSchema( me.dataSchema );
                String inputEventSchema = serdesProxy.serializeEventSchema( me.inputEventSchema);
                String outputEventSchema = serdesProxy.serializeEventSchema( me.outputEventSchema);

                attachment.buffer.clear();
                Presentation.writeVms( attachment.buffer, me, dataSchema, inputEventSchema, outputEventSchema );
                attachment.buffer.flip();

                attachment.channel.write(attachment.buffer, attachment, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, ConnectToExternalVmsProtocol attachment) {
                        attachment.state = State.PRESENTATION_SENT;
                        attachment.buffer.clear();
                        MemoryManager.releaseTemporaryDirectBuffer(attachment.buffer);
                    }

                    @Override
                    public void failed(Throwable exc, ConnectToExternalVmsProtocol attachment) {
                        // check if connection is still online. if so, try again
                        // otherwise, retry connection in a few minutes
                    }
                });

                channel.read(connMetadata.readBuffer, connMetadata, new VmsReadCompletionHandler() );

            }

            @Override
            public void failed(Throwable exc, ConnectToExternalVmsProtocol attachment) {
                // queue for later attempt
                // perhaps can use scheduled task
            }
        }

    }

    /**
     * The leader will let each VMS aware of their dependencies,
     * to which VMSs they have to connect to
     */
    private void connectToConsumerVMSs() {

        for(NetworkNode vms : this.consumerVms.values()) {

            try {

                InetSocketAddress address = new InetSocketAddress(vms.host, vms.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);

                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                ConnectToExternalVmsProtocol protocol = new ConnectToExternalVmsProtocol(channel, vms);

                channel.connect(address, protocol, protocol.connectCompletionHandler);

            } catch (IOException ignored) {
                issueQueue.add( new Issue(CANNOT_CONNECT_TO_NODE, vms.hashCode()) );
            }

        }
    }

    private class VmsReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            byte messageType = connectionMetadata.readBuffer.get(0);

            switch (messageType) {
                case (BATCH_OF_EVENTS) -> {



                }
                case (EVENT) -> {

                    // can only be event, skip reading the message type
                    connectionMetadata.readBuffer.position(1);

                    // data dependence or input event
                    TransactionEvent.Payload transactionEventPayload = TransactionEvent.read(connectionMetadata.readBuffer);

                    // send to scheduler
                    if (vmsMetadata.queueToEventMap().get(transactionEventPayload.event()) != null) {
                        vmsInternalChannels.transactionInputQueue().add(transactionEventPayload);
                    }

                } case ( BATCH_COMMIT_INFO) -> {

                    // received from a vms
                    // TODO finish

                }
                default ->
                    logger.warning("Unknown message type received from vms");
            }

//            else if(message == CONSUMER_SET){
//                // TODO read consumer set and set read handler again
//                ConsumerSet.read( connectionMetadata.readBuffer, serdesProxy );
//            }

            connectionMetadata.readBuffer.clear();
            connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);

        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            if (connectionMetadata.channel.isOpen()){
                connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);
            }
        }
    }

    /**
     * On an connection attempt, it is unknown what is the type of node
     * attempting the connection. We find out after the first read.
     */
    private class UnknownNodeReadCompletionHandler implements CompletionHandler<Integer, Void> {

        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;

        public UnknownNodeReadCompletionHandler(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            this.channel = channel;
            this.buffer = buffer;
        }

        @Override
        public void completed(Integer result, Void void_) {

            logger.info("Starting process for processing presentation message.");

            // message identifier
            byte messageIdentifier = this.buffer.get(0);
            if(messageIdentifier != PRESENTATION){
                logger.warning("A node is trying to connect without a presentation message");
                this.buffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
                try { channel.close(); } catch (IOException ignored) {}
                return;
            }

            byte nodeTypeIdentifier = this.buffer.get(1);
            this.buffer.position(2);

            switch (nodeTypeIdentifier) {

                case (SERVER_TYPE) -> {
                    ConnectionFromLeaderProtocol connectionFromLeader = new ConnectionFromLeaderProtocol(channel, this.buffer);
                    // could be a virtual thread
                    connectionFromLeader.processLeaderPresentation();
                }
                case (VMS_TYPE) -> {

                    // then it is a vms intending to connect due to a data/event
                    // that should be delivered to this vms
                    VmsIdentifier producerVms = Presentation.readVms(this.buffer, serdesProxy);
                    this.buffer.clear();

                    ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer();

                    ConnectionMetadata connMetadata = new ConnectionMetadata(
                            producerVms.hashCode(),
                            ConnectionMetadata.NodeType.VMS,
                            this.buffer,
                            writeBuffer,
                            this.channel,
                            new Semaphore(1)
                    );

                    vmsConnectionMetadataMap.put(producerVms.hashCode(), connMetadata);

                    // setup event receiving for this vms
                    this.channel.read(this.buffer, connMetadata, new VmsReadCompletionHandler());

                }
                case CLIENT -> {
                    // used for bulk data loading for now (may be used for tests later)

                    String tableName = Presentation.readClient(this.buffer);
                    this.buffer.clear();

                    ConnectionMetadata connMetadata = new ConnectionMetadata(
                            tableName.hashCode(),
                            ConnectionMetadata.NodeType.CLIENT,
                            this.buffer,
                            null,
                            this.channel,
                            null
                    );

                    BulkDataLoader bulkDataLoader = (BulkDataLoader) vmsMetadata.loadedVmsInstances().get("data_loader");
                    if(bulkDataLoader != null)
                        bulkDataLoader.init( tableName, connMetadata );
                    else
                        logger.warning("Data loader is not loaded in the runtime.");

                }
                default -> {
                    logger.warning("Presentation message from unknown source:" + nodeTypeIdentifier);
                    this.buffer.clear();
                    MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
                    try {
                        this.channel.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        @Override
        public void failed(Throwable exc, Void void_) {
            logger.warning("Error on processing presentation message!");
        }

    }

    /**
     * Class is iteratively called by the socket pool threads.
     */
    private class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {

            logger.info("An unknown host has started a connection attempt.");

            final ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(1024);

            try {

                logger.info("Remote address: "+channel.getRemoteAddress().toString());

                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                channel.read( buffer, null,
                        new UnknownNodeReadCompletionHandler(channel, buffer) );

                logger.info("Read handler for unknown node has been setup: "+channel.getRemoteAddress());

            } catch(Exception e){
                logger.info("Accept handler for unknown node caught exception: "+e.getMessage());
                MemoryManager.releaseTemporaryDirectBuffer(buffer);
            } finally {
                logger.info("Accept handler set up again for listening.");
                // continue listening
                serverSocket.accept(null, this);
            }

        }

        @Override
        public void failed(Throwable exc, Void attachment) {

            logger.warning("Error on accepting connection: "+ exc.getMessage());

            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            } else {
                logger.warning("Cannot set up ACCEPT again =(");
            }
        }

    }

    private class ConnectionFromLeaderProtocol {

        private State state;
        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;
        public final CompletionHandler<Integer, Void> writeCompletionHandler;

        public ConnectionFromLeaderProtocol(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            this.state = State.PRESENTATION_RECEIVED;
            this.channel = channel;
            this.writeCompletionHandler = new WriteCompletionHandler();
            this.buffer = buffer;
        }

        private enum State {
            PRESENTATION_RECEIVED,
            PRESENTATION_PROCESSED,
            PRESENTATION_SENT
        }

        /**
         * Should be void or ConnectionFromLeaderProtocol??? only testing to know...
         */
        private class WriteCompletionHandler implements CompletionHandler<Integer,Void> {

            @Override
            public void completed(Integer result, Void attachment) {
                state = State.PRESENTATION_SENT;
                channel.read(buffer, leaderConnectionMetadata, new LeaderReadCompletionHandler() );
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                if(!channel.isOpen()) {
                    leader.off();
                }
                // else what to do try again?
            }
        }

        public void processLeaderPresentation() {

            boolean includeMetadata = this.buffer.get() == YES;

            // leader has disconnected, or new leader
            leader = Presentation.readServer(this.buffer, serdesProxy);

            // only connects to all VMSs on first leader connection

            if(leaderConnectionMetadata == null) {

                // then setup connection metadata and read completion handler
                leaderConnectionMetadata = new ConnectionMetadata(
                        leader.hashCode(),
                        ConnectionMetadata.NodeType.SERVER,
                        buffer,
                        MemoryManager.getTemporaryDirectBuffer(),
                        channel,
                        new Semaphore(1)
                );

            } else {
                // considering the leader has replicated the metadata before failing
                // so no need to send metadata again. but it may be necessary...
                // what if the tid and batch id is necessary. the replica may not be
                // sync with last leader...
                leaderConnectionMetadata.channel = channel; // update channel
            }
            leader.on();

            this.buffer.clear();

            if(includeMetadata) {
                String vmsDataSchemaStr = serdesProxy.serializeDataSchema(me.dataSchema);
                String vmsInputEventSchemaStr = serdesProxy.serializeEventSchema(me.inputEventSchema);
                String vmsOutputEventSchemaStr = serdesProxy.serializeEventSchema(me.outputEventSchema);

                Presentation.writeVms(this.buffer, me, vmsDataSchemaStr, vmsInputEventSchemaStr, vmsOutputEventSchemaStr);
                // the protocol requires the leader to wait for the metadata in order to start sending messages
            } else {
                Presentation.writeVms(this.buffer, me);
            }

            this.buffer.flip();
            this.state = State.PRESENTATION_PROCESSED;
            this.channel.write( this.buffer, null, this.writeCompletionHandler );

        }

    }

    private class LeaderReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            connectionMetadata.readBuffer.position(0);
            byte messageType = connectionMetadata.readBuffer.get();

            logger.info("Leader has sent a message type: "+messageType);

            // receive input events
            switch (messageType) {

                case (BATCH_OF_EVENTS) -> {

                    // to increase performance, one would buffer this buffer for processing and then read from another buffer

                    int count = connectionMetadata.readBuffer.getInt();

                    List<TransactionEvent.Payload> payloads = new ArrayList<>(count);

                    TransactionEvent.Payload payload;

                    // extract events batched
                    for (int i = 0; i < count - 1; i++) {
                        // move offset to discard message type
                        connectionMetadata.readBuffer.get();
                        payload = TransactionEvent.read(connectionMetadata.readBuffer);
                        if (vmsMetadata.queueToEventMap().get(payload.event()) != null)
                            payloads.add(payload);
                    }

                    byte eventType = connectionMetadata.readBuffer.get();
                    if (eventType == BATCH_COMMIT_INFO) {

                        // it means this VMS is a terminal node in the current batch to be committed
                        BatchCommitInfo.Payload bPayload = BatchCommitInfo.read(connectionMetadata.readBuffer);
                        processNewBatchInfo(bPayload);

                    } else { // then it is still event
                        payload = TransactionEvent.read(connectionMetadata.readBuffer);
                        if (vmsMetadata.queueToEventMap().get(payload.event()) != null)
                            payloads.add(payload);
                    }

                    // add after to make sure the batch context map is filled by the time the output event is generated
                    vmsInternalChannels.transactionInputQueue().addAll(payloads);
                }
                case (BATCH_COMMIT_REQUEST) -> {

                    // a batch commit queue from a batch current + 1 can arrive before this vms moves next? yes

                    BatchCommitRequest.Payload payload = BatchCommitRequest.read(connectionMetadata.readBuffer);

                    if(payload.batch() == currentBatch.batch) {
                        vmsInternalChannels.batchCommitRequestQueue().add(currentBatch);
                    } else {
                        // buffer it
                        batchContextMap.get( payload.batch() ).requestPayload = payload;
                    }

                }
                case (EVENT) -> {

                    TransactionEvent.Payload payload = TransactionEvent.read(connectionMetadata.readBuffer);

                    // send to scheduler.... drop if the event cannot be processed (not an input event in this vms)
                    if (vmsMetadata.queueToEventMap().get(payload.event()) != null) {
                        vmsInternalChannels.transactionInputQueue().add(payload);
                    }

                }
                case (TX_ABORT) -> {
                    TransactionAbort.Payload transactionAbortReq = TransactionAbort.read(connectionMetadata.readBuffer);
                    vmsInternalChannels.transactionAbortInputQueue().add(transactionAbortReq);
                }
//            else if (messageType == BATCH_ABORT_REQUEST){
//                // some new leader request to rollback to last batch commit
//                BatchAbortRequest.Payload batchAbortReq = BatchAbortRequest.read( connectionMetadata.readBuffer );
//                vmsInternalChannels.batchAbortQueue().add(batchAbortReq);
//            }
                case (CONSUMER_SET) -> {

                    // read
                    Map<String, NetworkNode> receivedConsumerVms = ConsumerSet.read(connectionMetadata.readBuffer, serdesProxy);

                    if (receivedConsumerVms != null) {
                        consumerVms.putAll(receivedConsumerVms);
                        connectToConsumerVMSs();
                    }

                }
            }

            connectionMetadata.readBuffer.clear();
            connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);
        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            if (connectionMetadata.channel.isOpen()){
                connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);
            } else {
                leader.off();
            }
        }

    }

}
