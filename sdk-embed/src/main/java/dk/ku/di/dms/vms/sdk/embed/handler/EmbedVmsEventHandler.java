package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.ConsumerVms;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.VmsIdentifier;
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
import dk.ku.di.dms.vms.sdk.core.scheduler.ISchedulerHandler;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionResult;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.ingest.BulkDataLoader;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.Issue;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
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
 * -
 * The virtual microservice don't know who is the coordinator. It should be passive.
 * The leader and followers must share a list of VMSs.
 * Could also try to adapt to JNI:
 * <a href="https://nachtimwald.com/2017/06/17/calling-java-from-c/">...</a>
 */
public final class EmbedVmsEventHandler extends SignalingStoppableRunnable {

    private static final int DEFAULT_DELAY_FOR_BATCH_SEND = 5000;

    private final ExecutorService executorService;

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
    // TODO the identification of a consumer today occurs via
    //  a 1:1 mapping between an output event and the vms
    //  what if a consumer has more than an event to receive?
    //      two entries, overhead in the message
    //  what if two consumers receive the same event?
    //      no way to map.
    //  thus, the value must be a set, not a single network node
    private final Map<String, ConsumerVms> consumerVms; // sent by coordinator

    // built while connecting to the consumers
    private final Map<Integer, ConnectionMetadata> consumerConnectionMetadataMap;

    // built dynamically as new producers request connection
    private final Map<Integer, ConnectionMetadata> producerConnectionMetadataMap;

    /** SERIALIZATION & DESERIALIZATION **/
    private final IVmsSerdesProxy serdesProxy;

    /** COORDINATOR **/
    private ServerIdentifier leader;
    private ConnectionMetadata leaderConnectionMetadata;

    /** INTERNAL STATE **/
    private BatchContext currentBatch;

    // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    public final ISchedulerHandler schedulerHandler;

    public static EmbedVmsEventHandler build(VmsEmbedInternalChannels vmsInternalChannels, // for communicating with other components
                                             VmsIdentifier me, // to identify which vms this is
                                             Map<String, ConsumerVms> consumerVms,
                                             VmsRuntimeMetadata vmsMetadata, // metadata about this vms
                                             IVmsSerdesProxy serdesProxy, // ser/des of objects
                                             ExecutorService executorService) throws IOException // for recurrent and continuous tasks
    {
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool(executorService);
        try (AsynchronousServerSocketChannel serverSocket = AsynchronousServerSocketChannel.open(group)) {
            SocketAddress address = new InetSocketAddress(me.host, me.port);
            serverSocket.bind(address);
            // once up, can connect to consumers
            return new EmbedVmsEventHandler(me,vmsMetadata,consumerVms,vmsInternalChannels,serdesProxy,serverSocket,group,executorService);
        }
    }

    private EmbedVmsEventHandler(VmsIdentifier me,
                                 VmsRuntimeMetadata vmsMetadata,
                                 Map<String, ConsumerVms> consumerVms,
                                 VmsEmbedInternalChannels vmsInternalChannels,
                                 IVmsSerdesProxy serdesProxy,
                                 AsynchronousServerSocketChannel serverSocket,
                                 AsynchronousChannelGroup group,
                                 ExecutorService executorService) {
        super();
        this.serverSocket = serverSocket;
        this.group = group;

        this.executorService = executorService;

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;

        this.vmsMetadata = vmsMetadata;
        this.consumerVms = Objects.requireNonNullElseGet(consumerVms, ConcurrentHashMap::new);
        this.consumerConnectionMetadataMap = new ConcurrentHashMap<>(10);
        this.producerConnectionMetadataMap = new ConcurrentHashMap<>(10);

        this.serdesProxy = serdesProxy;

        this.currentBatch = new BatchContext(me.lastBatch, me.lastTid);
        this.currentBatch.setStatus(BatchContext.Status.COMPLETION_INFORMED);
        this.batchContextMap = new ConcurrentHashMap<>(3);

        this.schedulerHandler = new EmbedSchedulerHandler();
    }

    /**
     * A thread that basically writes events to other VMSs and the Leader
     * Retrieves data from all output queues
     * -
     * All output queues must be read in order to send their data
     * -
     * A batch strategy for sending would involve sleeping until the next timeout for batch,
     * send and set up the next. Do that iteratively
     */

    private void eventLoop(){

        logger.info("Event handler has started running.");

        if(!this.consumerVms.isEmpty()){
            // then it is received from constructor, and we must initially contact them
            this.connectToConsumerVMSs(this.consumerVms);
        }

        // setup accept since we need to accept connections from the coordinator and other VMSs
        this.serverSocket.accept( null, new AcceptCompletionHandler());

        // must wait for the consumer set before starting receiving transactions
        // this is guaranteed by the coordinator, the one who is sending transaction requests

        logger.info("Accept handler has been setup.");

        // while not stopped
        while(this.isRunning()){

            //  write events to leader and VMSs...
            try {

//                if(!vmsInternalChannels.transactionAbortOutputQueue().isEmpty()){
//                    // TODO handle
//                }

                // it is better to get all the results of a given transaction instead of one by one. it must be atomic anyway
                if(!this.vmsInternalChannels.transactionOutputQueue().isEmpty()){

                    VmsTransactionResult txResult = vmsInternalChannels.transactionOutputQueue().take();

                    // handle
                    logger.info("New transaction result in event handler. TID = "+txResult.tid);

                    if(this.currentBatch.status() == BatchContext.Status.NEW && this.currentBatch.lastTid == txResult.tid){
                        // we need to alert the scheduler...
                        logger.info("The last TID for the current batch has arrived. Time to inform the coordinator about the completion.");
                        // this.vmsInternalChannels.batchContextQueue().add( this.currentBatch );

                        // many outputs from the same transaction may arrive here, but can only send the batch commit once
                        this.currentBatch.setStatus(BatchContext.Status.COMPLETED);

                        this.informBatchCompletion();
                    }

                    // what could go wrong in terms of interleaving? what if this tid is the last of a given batch?
                    // may not happen because the batch commit info is processed before events are sent to the scheduler
                    // to remove possibility of interleaving completely, it is better to call

                    // just send events to appropriate targets
                    for(OutboundEventResult outputEvent : txResult.resultTasks){
                        this.processOutputEvent(outputEvent);
                    }

                    this.moveBatchIfNecessary();

                }

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Problem on handling event on event handler:"+e.getMessage());
            }

        }

        logger.info("Event handler has finished execution.");

    }

    @Override
    public void run() {
        this.eventLoop();
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

    private class EmbedSchedulerHandler implements ISchedulerHandler {

        @Override
        public Future<?> run() {
            BatchContext currentBatch = vmsInternalChannels.batchCommitRequestQueue().remove();

            currentBatch.setStatus(BatchContext.Status.LOGGING);

            return executorService.submit(() -> {
                // of course, I do not need to stop the scheduler on commit
                // I need to make access to the data versions data race free
                // so new transactions get data versions from the version map or the store
                // FIXME find later how to call transaction facade here: transactionFacade.log();

                currentBatch.setStatus(BatchContext.Status.COMMITTED);
            });

        }

        @Override
        public boolean conditionHolds() {
            return !vmsInternalChannels.batchCommitRequestQueue().isEmpty();
        }
    }

    /**
     *
     * @param outputEvent the event to be sent to the respective consumer vms
     */
    private void processOutputEvent(OutboundEventResult outputEvent){

        if(outputEvent.outputQueue() == null) return; // it is a void method that executed, nothing to send
        ConsumerVms consumerVms = this.consumerVms.get(outputEvent.outputQueue());

        if(consumerVms == null){
            logger.warning(
                    "An output event (queue: "+outputEvent.outputQueue()+") has no target virtual microservice.");
            return;
        }

        logger.info("An output event (queue: "+outputEvent.outputQueue()+") will be sent to vms: "+consumerVms);

        ConnectionMetadata connectionMetadata = this.consumerConnectionMetadataMap.get(consumerVms.hashCode());

        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(outputEvent.outputQueue());
        String objStr = this.serdesProxy.serialize(outputEvent.output(), clazz);

        if(connectionMetadata.timer == null){
            // set up event sender timer task
            connectionMetadata.timer = new Timer ("event-sender-timer", true);
            connectionMetadata.timer.schedule(new EventSenderTask(consumerVms, connectionMetadata), System.currentTimeMillis() + DEFAULT_DELAY_FOR_BATCH_SEND );
        }

        // concurrency issue if add to a list
        BlockingQueue<TransactionEvent.Payload> queue = consumerVms.transactionEventsPerBatch.computeIfAbsent(outputEvent.batch(), (x) -> new LinkedBlockingDeque<>());
        queue.add( TransactionEvent.of(outputEvent.tid(), outputEvent.lastTid(), outputEvent.batch(), outputEvent.outputQueue(), objStr ) );

    }

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
    private static final class EventSenderTask extends TimerTask {

        private final ConsumerVms consumerVms;
        private final ConnectionMetadata connectionMetadata;

        public EventSenderTask(ConsumerVms consumerVms, ConnectionMetadata connectionMetadata){
            this.consumerVms = consumerVms;
            this.connectionMetadata = connectionMetadata;
        }

        private int assembleBatchPayload(int lastIndex, List<TransactionEvent.Payload> events){
            int bufferSize = this.connectionMetadata.writeBuffer.capacity();

            this.connectionMetadata.writeBuffer.clear();
            this.connectionMetadata.writeBuffer.put(BATCH_OF_EVENTS);
            this.connectionMetadata.writeBuffer.position(5);

            // batch them all in the buffer,
            // until buffer capacity is reached or elements are all sent
            int count = 0;
            // int idx = 0;
            int remainingBytes = bufferSize - 1 - Integer.BYTES;
            int idx = lastIndex;

            while(idx > 0 && remainingBytes > events.get(idx).totalSize()){
                TransactionEvent.write( connectionMetadata.writeBuffer, events.get(idx) );
                remainingBytes = remainingBytes - events.get(idx).totalSize();
                idx--;
                count++;
            }

            connectionMetadata.writeBuffer.mark();
            connectionMetadata.writeBuffer.putInt(1, count);
            connectionMetadata.writeBuffer.reset();

            connectionMetadata.writeBuffer.flip();

            return idx;
        }

        @Override
        public void run() {

            // find the smallest batch. to avoid synchronizing with main thread
            long batchToSend = Long.MAX_VALUE;
            for(long batchId  : this.consumerVms.transactionEventsPerBatch.keySet()){
                if(batchId < batchToSend) batchToSend = batchId;
            }

            // there will always be a batch if this point of code is run
            List<TransactionEvent.Payload> events = new ArrayList<>(this.consumerVms.transactionEventsPerBatch.get(batchToSend).size());
            this.consumerVms.transactionEventsPerBatch.get(batchToSend).drainTo(events);

            int remaining = events.size() - 1;

            while(remaining > 0){
                remaining = this.assembleBatchPayload( remaining, events);
                try {
                    this.connectionMetadata.channel.write(this.connectionMetadata.writeBuffer).get();
                } catch (InterruptedException | ExecutionException e) {
                    // return non-processed events to original location or what?
                    if(!this.connectionMetadata.channel.isOpen()){
                        this.consumerVms.off();
                    }
                    break; // force exit loop
                }
            }

            // schedule again the timer
            if(this.consumerVms.isActive()) {
                this.connectionMetadata.timer.schedule(this, System.currentTimeMillis() + DEFAULT_DELAY_FOR_BATCH_SEND);
            }

        }

    }

    /**
     * In the future, we possibly need to send events to the leader.
     * But to be honest, it is just a matter of the leader sending
     * itself as a consumer. That would incur in another connection
     * but may be worthwhile since the purposes could be different...
     * Otherwise, we can make this class recognize whether it is the
     * leader and then just reuse the connection. The only thing is that
     * would require lock if the batch procedures and the event sending to
     * leader are not synchronized. More to come...
     */
    private void sendEventToLeader(OutboundEventResult outputEvent) throws InterruptedException {
        this.leaderConnectionMetadata.writeLock.acquire();
        this.leaderConnectionMetadata.writeBuffer.clear();

        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(outputEvent.outputQueue());

        String objStr = this.serdesProxy.serialize(outputEvent.output(), clazz);

        TransactionEvent.write( this.leaderConnectionMetadata.writeBuffer, outputEvent.tid(), 0, 0, outputEvent.outputQueue(), objStr );
        this.leaderConnectionMetadata.writeBuffer.flip();

        this.leaderConnectionMetadata.channel.write(this.leaderConnectionMetadata.writeBuffer, null,
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

                if(producerConnectionMetadataMap.containsKey(node.hashCode())){
                    logger.warning("The node "+node.host+" "+node.port+" already contains a connection as a producer");
                }

                consumerConnectionMetadataMap.put(node.hashCode(), connMetadata);

                String dataSchema = serdesProxy.serializeDataSchema(me.dataSchema);
                String inputEventSchema = serdesProxy.serializeEventSchema(me.inputEventSchema);
                String outputEventSchema = serdesProxy.serializeEventSchema(me.outputEventSchema);

                attachment.buffer.clear();
                Presentation.writeVms( attachment.buffer, me, me.vmsIdentifier, me.lastTid, me.lastBatch, dataSchema, inputEventSchema, outputEventSchema );
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
                        issueQueue.add( new Issue(CANNOT_CONNECT_TO_NODE, attachment.node.hashCode()) );
                    }
                });

                channel.read(connMetadata.readBuffer, connMetadata, new VmsReadCompletionHandler() );

            }

            @Override
            public void failed(Throwable exc, ConnectToExternalVmsProtocol attachment) {
                // queue for later attempt
                // perhaps can use scheduled task
                issueQueue.add( new Issue(CANNOT_CONNECT_TO_NODE, attachment.node.hashCode()) );
            }
        }

    }

    /**
     * The leader will let each VMS aware of their dependencies,
     * to which VMSs they have to connect to
     */
    private void connectToConsumerVMSs(Map<String, ConsumerVms> consumerSet) {

        for(NetworkNode vms : consumerSet.values()) {

            try {

                InetSocketAddress address = new InetSocketAddress(vms.host, vms.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);

                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                ConnectToExternalVmsProtocol protocol = new ConnectToExternalVmsProtocol(channel, vms);

                channel.connect(address, protocol, protocol.connectCompletionHandler);

            } catch (IOException ignored) {
                this.issueQueue.add( new Issue(CANNOT_CONNECT_TO_NODE, vms.hashCode()) );
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

                } case (BATCH_COMMIT_INFO) -> {

                    // received from a vms
                    // TODO finish

                }
                default ->
                    logger.warning("Unknown message type received from vms");
            }

//            else if(message == CONSUMER_SET){
//                // TODO read consumer set and set read handler again. why here, isn't the leader responsible for that?
//                ConsumerSet.read( connectionMetadata.readBuffer, serdesProxy );
//            }

            connectionMetadata.readBuffer.clear();
            connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);

        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            if (connectionMetadata.channel.isOpen()){
                connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);
            } // else no nothing. upon a new connection this metadata can be recycled
        }
    }

    /**
     * On a connection attempt, it is unknown what is the type of node
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

                    // what if a vms is both producer to and consumer from this vms?
                    if(consumerConnectionMetadataMap.containsKey(producerVms.hashCode())){
                        logger.warning("The node "+producerVms.host+" "+producerVms.port+" already contains a connection as a consumer");
                    }

                    producerConnectionMetadataMap.put(producerVms.hashCode(), connMetadata);

                    // setup event receiving for this vms
                    this.channel.read(this.buffer, connMetadata, new VmsReadCompletionHandler());

                }
                case CLIENT -> {
                    // used for bulk data loading for now (maybe used for tests later)

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

            String message = exc.getMessage();
            message = message == null ? exc.getCause() instanceof ClosedChannelException ? "Connection is closed" : "No cause could be identified" : "No cause could be identified";
            logger.warning("Error on accepting connection: "+ message);

            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            } else {
                logger.warning("Socket is not open anymore. Cannot set up accept again");
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
            leader = Presentation.readServer(this.buffer);

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

                Presentation.writeVms(this.buffer, me, me.vmsIdentifier, me.lastTid, me.lastBatch, vmsDataSchemaStr, vmsInputEventSchemaStr, vmsOutputEventSchemaStr);
                // the protocol requires the leader to wait for the metadata in order to start sending messages
            } else {
                Presentation.writeVms(this.buffer, me, me.vmsIdentifier, me.lastTid, me.lastBatch);
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
//                // some new leader request to roll back to last batch commit
//                BatchAbortRequest.Payload batchAbortReq = BatchAbortRequest.read( connectionMetadata.readBuffer );
//                vmsInternalChannels.batchAbortQueue().add(batchAbortReq);
//            }
                case (CONSUMER_SET) -> {

                    // read
                    Map<String, ConsumerVms> receivedConsumerVms = ConsumerSet.read(connectionMetadata.readBuffer, serdesProxy);

                    if (receivedConsumerVms != null) {
                        consumerVms.putAll(receivedConsumerVms);
                        // process only the new ones
                        connectToConsumerVMSs(receivedConsumerVms);
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
