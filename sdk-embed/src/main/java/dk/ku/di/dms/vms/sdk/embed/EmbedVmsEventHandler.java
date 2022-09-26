package dk.ku.di.dms.vms.sdk.embed;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
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
 * https://nachtimwald.com/2017/06/17/calling-java-from-c/
 */
public final class EmbedVmsEventHandler extends SignalingStoppableRunnable {

    /** EXECUTOR SERVICE (for socket channels) **/
    private final ExecutorService taskExecutor;

    /** SERVER SOCKET **/
    // other VMSs may want to connect in order to send events
    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

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
        this.currentBatch.setStatus(BatchContext.Status.DONE);
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

        // setup accept since we need to accept connections from the coordinator and other VMSs
        serverSocket.accept( null, new AcceptCompletionHandler());

        // must wait for the consumer set before starting receiving transactions

        // while not stopped
        while(isRunning()){

            //  write events to leader and VMSs...
            try {

                if(!vmsInternalChannels.transactionAbortOutputQueue().isEmpty()){
                    // handle

                }

                if(!vmsInternalChannels.transactionOutputQueue().isEmpty()){
                    // handle
                    logger.info("New transaction output in event handler!!!");

                    OutboundEventResult outputEvent = vmsInternalChannels.transactionOutputQueue().take();

                    // send to a vms
                    if(!outputEvent.terminal()){

                        NetworkNode vms = consumerVms.get(outputEvent.outputQueue());

                        if(vms == null){
                            logger.warning(
                                    "A output event (queue: "+outputEvent.outputQueue()+") has no target virtual microservice.");
                            continue;
                        }

                        // ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get(vms.hashCode());

                        // connectionMetadata.readBuffer.flip()

                        // then must check whether the current tid is higher than the current batch last tid. if higher, process batch commit
                        // the idea is to avoid having to wait for the batch commit request to arrive. we can continue process transactions
                        // as long as we log the write state. since the terminals have initiated, that means the transactions have executed
                        // successfully in the previous nodes

                    } else {

                        // if there is a output for a given tid, that is because all of the task of the tid have succeeded
                        if(this.currentBatch.lastTid == outputEvent.tid()){
                            // we need to alert the scheduler...
                            this.vmsInternalChannels.batchContextQueue().add( this.currentBatch );

                        }

                        logger.info("Terminal, but there may be a payload to send to leader...");
                        // send to leader (if there is a payload)
                        // sendEventToLeader(outputEvent);

                    }

                }

                // do we have a new batch request?
                checkBatchRelatedEvents();

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Problem on handling event on event handler:"+e.getMessage());
            }

        }

    }

    private void checkBatchRelatedEvents() {

        if(this.currentBatch.status() == BatchContext.Status.COMMITTED){

            this.currentBatch.setStatus(BatchContext.Status.REPLYING);

            this.leaderConnectionMetadata.writeLock.acquireUninterruptibly();
            this.leaderConnectionMetadata.writeBuffer.clear();
            BatchComplete.write( leaderConnectionMetadata.writeBuffer, this.currentBatch.batch, this.me );

            this.leaderConnectionMetadata.writeBuffer.flip();

//            this.leaderConnectionMetadata.channel.write(this.leaderConnectionMetadata.writeBuffer).get();
//            this.leaderConnectionMetadata.writeBuffer.clear();
//            this.leaderConnectionMetadata.writeLock.release();

            this.leaderConnectionMetadata.channel.write(this.leaderConnectionMetadata.writeBuffer, null,
                    new CompletionHandler<>() {

                        @Override
                        public void completed(Integer result, Object attachment) {
                            currentBatch.setStatus(BatchContext.Status.DONE);
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

        // the current batch is finished and at the same time do we have a new one, so we can move on the batch offset?
        var peek = vmsInternalChannels.newBatchCommitRequestQueue().peek();
        if(peek != null && this.currentBatch.status() == BatchContext.Status.DONE && peek.batch() == currentBatch.batch + 1){

            // remove from queue
            vmsInternalChannels.newBatchCommitRequestQueue().poll();

            BatchContext batchContext = new BatchContext(peek.batch(), peek.tid());
            this.batchContextMap.put(peek.batch(), batchContext);
            this.currentBatch = batchContext;

        }

    }

    private void processBatchCommitIfNecessary(OutboundEventResult outputEvent) throws InterruptedException {

        // send batch complete to leader and increment batch
        // it may be the case that a vms does not participate in a batch
        // either way it must receive a batch commit request since
        // single vms transaction may have executed own code
//        else {
//
//            // is this event the last tid of some future batch?
//            BatchContext currBatchCtx = currentBatch;
//            // find the previous of this batch commit and put on the next
//            while(currBatchCtx.next != null && currBatchCtx.lastTid != outputEvent.tid()){
//                currBatchCtx = currBatchCtx.next;
//            }
//            if(currBatchCtx.lastTid == outputEvent.tid()){
//                // mark this batch to be committed after the current one
//                logger.info("Batch ID: "+currBatchCtx.batch+" scheduled to run after current batch.");
//            }
//
//        }

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

            byte message = connectionMetadata.readBuffer.get(0);

            if (message == EVENT){

                // can only be event, skip reading the message type
                connectionMetadata.readBuffer.position(1);

                // data dependence or input event
                TransactionEvent.Payload transactionEventPayload = TransactionEvent.read(connectionMetadata.readBuffer);

                // send to scheduler
                if (vmsMetadata.queueToEventMap().get(transactionEventPayload.event()) != null) {
                    vmsInternalChannels.transactionInputQueue().add(transactionEventPayload);
                }

                connectionMetadata.readBuffer.clear();
                connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);

            } else if(message == CONSUMER_SET){

                // TODO read consumer set and set read handler again

                connectionMetadata.readBuffer.clear();
                connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);

            }

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

            if(nodeTypeIdentifier == SERVER_TYPE){
                ConnectionFromLeaderProtocol connectionFromLeader = new ConnectionFromLeaderProtocol(channel, this.buffer);
                // could be a virtual thread
                connectionFromLeader.processLeaderPresentation();
            } else if(nodeTypeIdentifier == VMS_TYPE) {
                // then it is a vms intending to connect due to a data/event
                // that should be delivered to this vms

                this.buffer.clear();
                VmsIdentifier producerVms = Presentation.readVms(this.buffer, serdesProxy);
                this.buffer.clear();

                ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer();

                ConnectionMetadata connMetadata = new ConnectionMetadata(
                        producerVms.hashCode(),
                        ConnectionMetadata.NodeType.VMS,
                        this.buffer,
                        writeBuffer,
                        channel,
                        new Semaphore(1)
                );

                vmsConnectionMetadataMap.put( producerVms.hashCode(), connMetadata );

                // setup event receiving for this vms
                channel.read(this.buffer, connMetadata, new VmsReadCompletionHandler() );

            } else {
                logger.warning("Presentation message from unknown source:"+nodeTypeIdentifier);
                this.buffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
                try { channel.close(); } catch (IOException ignored) { }
            }
        }

        @Override
        public void failed(Throwable exc, Void void_) {
            // do nothing
        }

    }

    /**
     * Class is iteratively called by the socket pool threads.
     */
    private class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {

            final ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(1024);

            try {

                logger.info("An unknown host has started a connection attempt. Remote address: "+channel.getRemoteAddress());

                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                channel.read( buffer, null,
                        new UnknownNodeReadCompletionHandler(channel, buffer) );

                logger.info("Read handler for unknown node has been setup: "+channel.getRemoteAddress());

            } catch(Exception ignored){
                MemoryManager.releaseTemporaryDirectBuffer(buffer);
            } finally {
                // continue listening
                serverSocket.accept(null, this);
            }

        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
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
            if(messageType == BATCH_OF_EVENTS){

                // to increase performance, one would buffer this buffer for processing and then read from another buffer

                int count = connectionMetadata.readBuffer.getInt();

                List<TransactionEvent.Payload> payloads = new ArrayList<>(count);

                TransactionEvent.Payload payload;

                // extract events batched
                for(int i = 0; i < count - 1; i++){
                    // move offset to discard message type
                    connectionMetadata.readBuffer.get();
                    payload = TransactionEvent.read(connectionMetadata.readBuffer);
                    if(vmsMetadata.queueToEventMap().get( payload.event() ) != null)
                        payloads.add(payload);
                }

                byte eventType = connectionMetadata.readBuffer.get();
                if(eventType == BATCH_COMMIT_REQUEST){
                    // it means this VMS is a terminal node in the current batch to be committed
                    BatchCommitRequest.Payload bPayload = BatchCommitRequest.read( connectionMetadata.readBuffer );
                    vmsInternalChannels.newBatchCommitRequestQueue().add(bPayload);
                } else { // then it is still event
                    payload = TransactionEvent.read(connectionMetadata.readBuffer);
                    if(vmsMetadata.queueToEventMap().get( payload.event() ) != null)
                        payloads.add(payload);
                }

                // add after to make sure the batch context map is filled by the time the output event is generated
                vmsInternalChannels.transactionInputQueue().addAll(payloads);

            } else if(messageType == BATCH_COMMIT_REQUEST) {

                // it means this VMS is not a terminal node in the current batch to be committed
                // for now lets make the logic work first

                BatchCommitRequest.Payload payload = BatchCommitRequest.read( connectionMetadata.readBuffer );
                vmsInternalChannels.newBatchCommitRequestQueue().add(payload);

            } else if(messageType == EVENT) {

                TransactionEvent.Payload payload = TransactionEvent.read(connectionMetadata.readBuffer);

                // send to scheduler.... drop if the event cannot be processed (not an input event in this vms)
                if(vmsMetadata.queueToEventMap().get( payload.event() ) != null) {
                    vmsInternalChannels.transactionInputQueue().add(payload);
                }

            }
//            else if (messageType == BATCH_COMMIT_REQUEST){
//                // must send batch commit ok
//                BatchCommitRequest.Payload batchCommitReq = BatchCommitRequest.read( connectionMetadata.readBuffer );
//                connectionMetadata.readBuffer.clear();
//                vmsInternalChannels.batchCommitQueue().add( batchCommitReq );
//            }
            else if(messageType == TX_ABORT){
                TransactionAbort.Payload transactionAbortReq = TransactionAbort.read( connectionMetadata.readBuffer );
                vmsInternalChannels.transactionAbortInputQueue().add( transactionAbortReq );
            }
//            else if (messageType == BATCH_ABORT_REQUEST){
//                // some new leader request to rollback to last batch commit
//                BatchAbortRequest.Payload batchAbortReq = BatchAbortRequest.read( connectionMetadata.readBuffer );
//                vmsInternalChannels.batchAbortQueue().add(batchAbortReq);
//            }

            else if( messageType == CONSUMER_SET){

                // read
                var receivedConsumerVms = ConsumerSet.read(connectionMetadata.readBuffer, serdesProxy);

                if(receivedConsumerVms != null) {
                    consumerVms.putAll(receivedConsumerVms);
                    connectToConsumerVMSs();
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
