package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.api.interfaces.IHttpHandler;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.*;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.transaction.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.IVmsTransactionResult;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.JdkAsyncChannel;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static java.lang.System.Logger.Level.*;

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
public final class VmsEventHandler extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(VmsEventHandler.class.getName());
    
    /** SERVER SOCKET **/
    // other VMSs may want to connect in order to send events
    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    /** INTERNAL CHANNELS **/
    private final VmsEmbedInternalChannels vmsInternalChannels;

    /** VMS METADATA **/
    private final VmsNode me; // this merges network and semantic data about the vms
    private final VmsRuntimeMetadata vmsMetadata;

    /** EXTERNAL VMSs **/
    private final Map<String, List<IVmsContainer>> eventToConsumersMap;

    // built while connecting to the consumers
    private final Map<IdentifiableNode, IVmsContainer> consumerVmsContainerMap;

    // built dynamically as new producers request connection
    private final Map<Integer, ConnectionMetadata> producerConnectionMetadataMap;

    /** For checkpointing the state */
    private final ITransactionManager transactionManager;

    /** SERIALIZATION & DESERIALIZATION **/
    private final IVmsSerdesProxy serdesProxy;

    private final VmsHandlerOptions options;

    private final ILoggingHandler loggingHandler;

    private final IHttpHandler httpHandler;
    
    /** COORDINATOR **/
    private ServerNode leader;

    private ConnectionMetadata leaderConnectionMetadata;

    // the thread responsible to send data to the leader
    private LeaderWorker leaderWorker;

    // refer to what operation must be performed
    // private final BlockingQueue<Object> leaderWorkerQueue;

    // cannot be final, may differ across time and new leaders
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final Set<String> queuesLeaderSubscribesTo;

    /** INTERNAL STATE **/

    /*
     * When is the current batch updated to the next?
     * - When the last tid of this batch (for this VMS) finishes execution,
     *   if this VMS is a terminal in this batch, send the batch complete event to leader
     *   if this vms is not a terminal, must wait for a batch commit request from leader
     *   -- but this wait can entail low throughput (rethink that later)
     */
    private BatchContext currentBatch;

    // metadata about all non-committed batches.
    // when a batch commit finishes, it should be removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    /**
     * It marks how many TIDs that the scheduler has executed.
     * The scheduler is batch-agnostic. That means in order
     * to progress with the batch in the event handler, we need to check if the
     * batch has completed using the number of TIDs executed.
     * It must be separated from batchContextMap due to different timing of batch context sending
     */
    public final Map<Long, BatchMetadata> volatileBatchMetadataMap;

    public static final class BatchMetadata {
        public int numberTIDsExecuted;
        public long maxTidExecuted;
    }

    private final Map<Long, Long> batchToNextBatchMap;

    /**
     * It is necessary a way to store the tid received to a
     * corresponding dependence map.
     */
    private final Map<Long, Map<String, Long>> tidToPrecedenceMap;

    public static VmsEventHandler build(// to identify which vms this is
                                        VmsNode me,
                                        // to checkpoint private state
                                        ITransactionManager transactionalHandler,
                                        // for communicating with other components
                                        VmsEmbedInternalChannels vmsInternalChannels,
                                        // metadata about this vms
                                        VmsRuntimeMetadata vmsMetadata,
                                        VmsApplicationOptions options,
                                        ILoggingHandler loggingHandler,
                                        IHttpHandler httpHandler,
                                        // serialization/deserialization of objects
                                        IVmsSerdesProxy serdesProxy){
        try {
            return new VmsEventHandler(me, vmsMetadata,
                    transactionalHandler, vmsInternalChannels,
                    new VmsEventHandler.VmsHandlerOptions( options.maxSleep(), options.networkBufferSize(),
                            options.osBufferSize(), options.networkThreadPoolSize(), options.networkSendTimeout(),
                            options.vmsThreadPoolSize(), options.numVmsWorkers(), options.isLogging()),
                    loggingHandler, httpHandler, serdesProxy);
        } catch (IOException e){
            throw new RuntimeException("Error on setting up event handler: "+e.getCause()+ " "+ e.getMessage());
        }
    }

    public record VmsHandlerOptions(int maxSleep,
                            int networkBufferSize,
                            int osBufferSize,
                            int networkThreadPoolSize,
                            int networkSendTimeout,
                            int vmsThreadPoolSize,
                            int numVmsWorkers,
                            boolean logging) {}

    private VmsEventHandler(VmsNode me,
                            VmsRuntimeMetadata vmsMetadata,
                            ITransactionManager transactionManager,
                            VmsEmbedInternalChannels vmsInternalChannels,
                            VmsHandlerOptions options,
                            ILoggingHandler loggingHandler,
                            IHttpHandler httpHandler,
                            IVmsSerdesProxy serdesProxy) throws IOException {
        super();

        // network and executor
        if(options.networkThreadPoolSize > 0){
            // at least two, one for acceptor and one for new events
            this.group = AsynchronousChannelGroup.withFixedThreadPool(options.networkThreadPoolSize, Thread.ofPlatform().factory());
            this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        } else {
            // by default, server socket creates a cached thread pool. better to avoid successive creation of threads
            this.group = null;
            this.serverSocket = AsynchronousServerSocketChannel.open(null);
        }
        this.serverSocket.bind(me.asInetSocketAddress());

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;

        this.vmsMetadata = vmsMetadata;
        // no concurrent threads modifying them
        this.eventToConsumersMap = new HashMap<>();
        this.consumerVmsContainerMap = new HashMap<>();
        // concurrent threads modifying it
        this.producerConnectionMetadataMap = new ConcurrentHashMap<>();

        this.serdesProxy = serdesProxy;

        this.currentBatch = BatchContext.buildAsStarter(me.batch, me.previousBatch, me.numberOfTIDsCurrentBatch);
        this.currentBatch.setStatus(BatchContext.BATCH_COMMITTED);
        this.batchContextMap = new ConcurrentHashMap<>();
        this.volatileBatchMetadataMap = new HashMap<>();
        this.batchToNextBatchMap = new ConcurrentHashMap<>();
        this.tidToPrecedenceMap = new ConcurrentHashMap<>();

        this.transactionManager = transactionManager;

        // set leader off at the start
        this.leader = new ServerNode("0.0.0.0",0);
        this.leader.off();

        this.queuesLeaderSubscribesTo = new HashSet<>();

        this.options = options;
        this.loggingHandler = loggingHandler;
        this.httpHandler = httpHandler;
    }

    @Override
    public void run() {
        LOGGER.log(INFO,this.me.identifier+": Event handler has started");
        // setup accept since we need to accept connections from the coordinator and other VMSs
        this.serverSocket.accept(null, new AcceptCompletionHandler());
        LOGGER.log(INFO,this.me.identifier+": Accept handler has been setup");
        // init event loop
        this.eventLoop();
        this.failSafeClose();
        LOGGER.log(INFO,this.me.identifier+": Event handler has finished execution.");
    }

    /**
     * From <a href="https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html">...</a>
     * "The Java runtime automatically closes the input and output streams, the client socket,
     * and the server socket because they have been created in the try-with-resources statement."
     * Which means that different tests must bind to different addresses
     */
    private void failSafeClose(){
        // safe close
        try { if(this.serverSocket.isOpen()) this.serverSocket.close(); } catch (IOException ignored) {
            LOGGER.log(WARNING,this.me.identifier+": Could not close socket");
        }
    }

    /**
     * A thread that basically writes events to other VMSs and the Leader
     * Retrieves data from all output queues
     * All output queues must be read in order to send their data
     * A batch strategy for sending would involve sleeping until the next timeout for batch,
     * send and set up the next. Do that iteratively
     */
    private void eventLoop(){
        int pollTimeout = 1;
        IVmsTransactionResult txResult;
        while(this.isRunning()){
            try {

                while((txResult = this.vmsInternalChannels.transactionOutputQueue().poll()) == null) {
                     pollTimeout = Math.min(pollTimeout * 2, this.options.maxSleep);
                     this.giveUpCpu(pollTimeout);
                     this.moveBatchIfNecessary();
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;

                LOGGER.log(DEBUG,this.me.identifier+": New transaction result in event handler. TID = "+txResult.tid());

                // assuming is a simple transaction result, not complex, so no need to iterate
                OutboundEventResult outputEvent = txResult.getOutboundEventResult();

                // scheduler can be way ahead of the last batch committed
                BatchMetadata batchMetadata = this.volatileBatchMetadataMap.computeIfAbsent(outputEvent.batch(), ignored -> new BatchMetadata());
                batchMetadata.numberTIDsExecuted += 1;
                if(batchMetadata.maxTidExecuted < outputEvent.tid()){
                    batchMetadata.maxTidExecuted = outputEvent.tid();
                }

                // it is a void method that executed, nothing to send
                if (outputEvent.outputQueue() == null) continue;
                Map<String, Long> precedenceMap = this.tidToPrecedenceMap.get(txResult.tid());
                if (precedenceMap == null) {
                    LOGGER.log(WARNING,this.me.identifier + ": No precedence map found for TID: " + txResult.tid());
                    continue;
                }
                // remove ourselves (which also saves some bytes)
                precedenceMap.remove(this.me.identifier);
                String precedenceMapUpdated = this.serdesProxy.serializeMap(precedenceMap);
                this.processOutputEvent(outputEvent, precedenceMapUpdated);

                this.moveBatchIfNecessary();

            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+": Problem on handling event\n"+e);
            }
        }
    }

    private void connectToReceivedConsumerSet(Map<String, List<IdentifiableNode>> receivedConsumerVms) {
        Map<IdentifiableNode, List<String>> consumerToEventsMap = new HashMap<>();
        // build an indirect map
        for(Map.Entry<String,List<IdentifiableNode>> entry : receivedConsumerVms.entrySet()) {
            for(IdentifiableNode consumer : entry.getValue()){
                consumerToEventsMap.computeIfAbsent(consumer, (ignored) -> new ArrayList<>()).add(entry.getKey());
            }
        }
        for( Map.Entry<IdentifiableNode,List<String>> consumerEntry : consumerToEventsMap.entrySet() ) {
            // connect to more consumers...
            for(int i = 0; i < this.options.numVmsWorkers; i++){
                this.initConsumerVmsWorker(consumerEntry.getKey(), consumerEntry.getValue(), i);
            }
        }
    }

    private boolean currentBatchFinishedAllTIDs(){
        BatchMetadata batchMetadata = this.volatileBatchMetadataMap.get(this.currentBatch.batch);
        if(batchMetadata == null){
            return false;
        }
        return this.currentBatch.numberOfTIDsBatch == batchMetadata.numberTIDsExecuted;
    }

    /**
     * Can acknowledge batch completion even though no event from next batch has arrived
     * But if blocking, only upon a new event such method will be invoked
     * That will jeopardize the batch process
     * It may be the case that, due to an abort of the last tid, the last tid changes
     * the current code is not incorporating that
     */
    private void moveBatchIfNecessary(){
        // update if current batch is completed AND the next batch has already arrived
        if(this.currentBatch.isCompleted() && this.batchToNextBatchMap.containsKey( this.currentBatch.batch )){
            long nextBatchOffset = this.batchToNextBatchMap.get( this.currentBatch.batch );
            this.currentBatch = this.batchContextMap.get(nextBatchOffset);
        }
        // have we processed all the TIDs of this batch?
        if(this.currentBatch.isOpen() && this.currentBatchFinishedAllTIDs()){
            LOGGER.log(DEBUG,this.me.identifier+": All TIDs for the current batch ("+this.currentBatch.batch+") have been executed");
            // many outputs from the same transaction may arrive here, but can only send the batch commit once
            this.currentBatch.setStatus(BatchContext.BATCH_COMPLETED);
            // if terminal, must send batch complete
            if(this.currentBatch.terminal) {
                LOGGER.log(INFO,this.me.identifier+": Requesting leader worker to send batch ("+this.currentBatch.batch+") complete");
                // must be queued in case leader is off and comes back online
                this.leaderWorker.queueMessage(BatchComplete.of(this.currentBatch.batch, this.me.identifier));
            }
            // clear map --> WARN: lead to bug!
            // this.volatileBatchMetadataMap.remove(this.currentBatch.batch);
            // this is necessary to move the batch to committed and allow the batches to progress
            // ForkJoinPool.commonPool().execute(()-> checkpoint(this.volatileBatchMetadataMap.get(this.currentBatch.batch).maxTidExecuted));
        }
    }

    private static final boolean INFORM_BATCH_ACK = false;

    private void checkpoint(long maxTid) {
        this.currentBatch.setStatus(BatchContext.CHECKPOINTING);
        // of course, I do not need to stop the scheduler on commit
        // I need to make access to the data versions data race free
        // so new transactions get data versions from the version map or the store
        this.transactionManager.checkpoint(maxTid);
        this.currentBatch.setStatus(BatchContext.BATCH_COMMITTED);
        // it may not be necessary. the leader has already moved on at this point
        if(INFORM_BATCH_ACK) {
            this.leaderWorker.queueMessage(BatchCommitAck.of(this.currentBatch.batch, this.me.identifier));
        }
    }

    /**
     * It creates the payload to be sent downstream
     * @param outputEvent the event to be sent to the respective consumer vms
     */
    private void processOutputEvent(OutboundEventResult outputEvent, String precedenceMap){
        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(outputEvent.outputQueue());
        String objStr = this.serdesProxy.serialize(outputEvent.output(), clazz);

        /*
         * does the leader consumes this queue?
        if( this.queuesLeaderSubscribesTo.contains( outputEvent.outputQueue() ) ){
            logger.log(DEBUG,me.identifier+": An output event (queue: "+outputEvent.outputQueue()+") will be queued to leader");
            this.leaderWorkerQueue.add(new LeaderWorker.Message(SEND_EVENT, payload));
        }
        */

        List<IVmsContainer> consumerVMSs = this.eventToConsumersMap.get(outputEvent.outputQueue());
        if(consumerVMSs == null || consumerVMSs.isEmpty()){
            LOGGER.log(DEBUG,this.me.identifier+": An output event (queue: "+outputEvent.outputQueue()+") has no target virtual microservices.");
            return;
        }

        // right now just including the original precedence map. ideally we must reduce the size by removing this vms
        // however, modifying the map incurs deserialization and serialization costs
        TransactionEvent.PayloadRaw payload = TransactionEvent.of(
                outputEvent.tid(), outputEvent.batch(), outputEvent.outputQueue(), objStr, precedenceMap );

        for(IVmsContainer consumerVmsContainer : consumerVMSs) {
            LOGGER.log(DEBUG,this.me.identifier+": An output event (queue: " + outputEvent.outputQueue() + ") will be queued to VMS: " + consumerVmsContainer.identifier());
            consumerVmsContainer.queue(payload);
        }
    }

    public void initConsumerVmsWorker(IdentifiableNode node, List<String> outputEvents, int identifier){
        if(this.producerConnectionMetadataMap.containsKey(node.hashCode())){
            LOGGER.log(WARNING,"The node "+ node.host+" "+ node.port+" already contains a connection as a producer");
        }

        if(this.me.hashCode() == node.hashCode()){
            LOGGER.log(ERROR,this.me.identifier+" is receiving itself as consumer: "+ node.identifier);
            return;
        }

        ConsumerVmsWorker consumerVmsWorker = ConsumerVmsWorker.build(this.me, node,
                        () -> JdkAsyncChannel.create(this.group),
                        this.options,
                        this.loggingHandler,
                        this.serdesProxy
                        );
        Thread.ofPlatform().name("vms-consumer-"+node.identifier+"-"+identifier)
                .inheritInheritableThreadLocals(false)
                .start(consumerVmsWorker);

        if(!this.consumerVmsContainerMap.containsKey(node)){
            if(this.options.numVmsWorkers == 1) {
                this.consumerVmsContainerMap.put(node, consumerVmsWorker);
            } else {
                MultiVmsContainer multiVmsContainer = new MultiVmsContainer(consumerVmsWorker, node, this.options.numVmsWorkers);
                this.consumerVmsContainerMap.put(node, multiVmsContainer);
            }
            // add to tracked VMSs
            for (String outputEvent : outputEvents) {
                LOGGER.log(INFO,me.identifier+ " adding "+outputEvent+" to consumers map with "+node.identifier);
                this.eventToConsumersMap.computeIfAbsent(outputEvent, (ignored) -> new ArrayList<>());
                this.eventToConsumersMap.get(outputEvent).add(consumerVmsWorker);
            }
        } else {
            IVmsContainer vmsContainer = this.consumerVmsContainerMap.get(node);
            if(vmsContainer instanceof MultiVmsContainer multiVmsContainer){
                multiVmsContainer.addConsumerVms(consumerVmsWorker);
            } else {
                // stop previous, replace by the new one
                ((ConsumerVmsWorker)vmsContainer).stop();
                this.consumerVmsContainerMap.put(node, consumerVmsWorker);
            }
        }

        // set up read from consumer vms? we read nothing from consumer vms. maybe in the future can negotiate amount of data to avoid performance problems
        // channel.read(buffer, 0, new VmsReadCompletionHandler(this.node, connMetadata, buffer));
    }

    private final class HttpReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;
        private final ByteBuffer writeBuffer;

        public HttpReadCompletionHandler(ConnectionMetadata connectionMetadata, ByteBuffer readBuffer) {
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = readBuffer;
            this.writeBuffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize);
        }

        private record HttpRequestInternal(String httpMethod, String uri, String body) {}

        private static HttpRequestInternal parseRequest(String request){
            String[] requestLines = request.split("\r\n");
            String requestLine = requestLines[0];  // First line is the request line
            String[] requestLineParts = requestLine.split(" ");
            String method = requestLineParts[0];
            String url = requestLineParts[1];
            String httpVersion = requestLineParts[2];
            if(method.contentEquals("GET")){
                return new HttpRequestInternal(method, url, "");
            }
            // process header
            int i = 1;
            while (requestLines.length > i &&
                            !requestLines[i].isEmpty()) {
                    String[] headerParts = requestLines[i].split(": ");
                    i++;
                }
            StringBuilder body = new StringBuilder();
            for (i += 1; i < requestLines.length; i++) {
                    body.append(requestLines[i]).append("\r\n");
                }
            String payload = body.toString().trim();
            return new HttpRequestInternal(method, url, payload);
        }

        public void process(String request){
            try {
                HttpRequestInternal httpRequest = parseRequest(request);
                Future<Integer> ft = null;
                switch (httpRequest.httpMethod()){
                    case "GET" -> {
                        String respVms = httpHandler.get(httpRequest.uri());
                        byte[] respBytes = respVms.getBytes(StandardCharsets.UTF_8);
                        String response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                                + respBytes.length + "\r\n\r\n" + respVms;
                        this.writeBuffer.put(response.getBytes(StandardCharsets.UTF_8));
                        this.writeBuffer.flip();
                        ft = this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                    case "POST" -> {
                        httpHandler.post(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        ft = this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                    case "PATCH" -> {
                        httpHandler.patch(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        ft = this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                }
                if(ft != null){
                    int result = ft.get();
                    // send remaining to avoid http client to hang
                    while (result < this.writeBuffer.position()){
                        result += this.connectionMetadata.channel.write(this.writeBuffer).get();
                    }
                }
                this.writeBuffer.clear();
            } catch (Exception e){
                LOGGER.log(WARNING, me.identifier+": Error caught in HTTP handler.\n"+e);
                this.writeBuffer.clear();
                this.writeBuffer.put(ERROR_RESPONSE_BYTES);
                this.writeBuffer.flip();
                this.connectionMetadata.channel.write(writeBuffer);
            }
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private static final byte[] OK_RESPONSE_BYTES = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.UTF_8);

        private static final byte[] ERROR_RESPONSE_BYTES = "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 11\r\n\r\nBad Request".getBytes(StandardCharsets.UTF_8);

        @Override
        public void completed(Integer result, Integer attachment) {
            if(result == -1){
                this.readBuffer.clear();
                this.writeBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(this.readBuffer);
                MemoryManager.releaseTemporaryDirectBuffer(this.writeBuffer);
                LOGGER.log(DEBUG,me.identifier+": HTTP client has disconnected!");
                return;
            }
            this.readBuffer.flip();
            String request = StandardCharsets.UTF_8.decode(this.readBuffer).toString();
            this.process(request);
        }

        @Override
        public void failed(Throwable exc, Integer attachment) {
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }
    }

    /**
     * The completion handler must execute fast
     */
    @SuppressWarnings("SequencedCollectionMethodCanBeUsed")
    private final class VmsReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        // the VMS sending events to me
        private final IdentifiableNode node;
        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;

        public VmsReadCompletionHandler(IdentifiableNode node,
                                        ConnectionMetadata connectionMetadata,
                                        ByteBuffer byteBuffer){
            this.node = node;
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = byteBuffer;
            LIST_BUFFER.add(new ArrayList<>(1024));
        }

        @Override
        public void completed(Integer result, Integer startPos) {
            if(result == -1){
                // end-of-stream signal, no more data can be read
                LOGGER.log(WARNING,me.identifier+": VMS "+node.identifier+" has disconnected!");
                try {
                    this.connectionMetadata.channel.close();
                } catch (IOException ignored) { }
                return;
            }
            if(startPos == 0){
                this.readBuffer.flip();
            }
            byte messageType = readBuffer.get();
            switch (messageType) {
                case (BATCH_OF_EVENTS) -> {
                    int bufferSize = this.getBufferSize();
                    if(this.readBuffer.remaining() < bufferSize){
                        this.fetchMoreBytes(startPos);
                        return;
                    }
                    this.processBatchOfEvents(this.readBuffer);
                }
                case (EVENT) -> {
                    int bufferSize = this.getBufferSize();
                    if(this.readBuffer.remaining() < bufferSize){
                        this.fetchMoreBytes(startPos);
                        return;
                    }
                    this.processSingleEvent(readBuffer);
                }
                default -> LOGGER.log(ERROR,me.identifier+": Unknown message type "+messageType+" received from: "+node.identifier);
            }
            if(readBuffer.hasRemaining()){
                this.completed(result, this.readBuffer.position());
            } else {
                this.setUpNewRead();
            }
        }

        private int getBufferSize() {
            int bufferSize = Integer.MAX_VALUE;
            // check if we can read an integer
            if(this.readBuffer.remaining() > Integer.BYTES) {
                // size of the batch
                bufferSize = this.readBuffer.getInt();
                // discard message type and size of batch from the total size since it has already been read
                bufferSize -= 1 + Integer.BYTES;
            }
            return bufferSize;
        }

        private void fetchMoreBytes(Integer startPos) {
            this.readBuffer.position(startPos);
            this.readBuffer.compact();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void setUpNewRead() {
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void processSingleEvent(ByteBuffer readBuffer) {
            try {
                TransactionEvent.Payload payload = TransactionEvent.read(readBuffer);
                LOGGER.log(DEBUG,me.identifier+": 1 event received from "+node.identifier+"\n"+payload);
                // send to scheduler
                if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                    InboundEvent inboundEvent = buildInboundEvent(payload);
                    vmsInternalChannels.transactionInputQueue().add(inboundEvent);
                }
            } catch (Exception e) {
                if(e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading event: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            }
        }

        private void processBatchOfEvents(ByteBuffer readBuffer) {
            List<InboundEvent> inboundEvents = LIST_BUFFER.poll();
            if(inboundEvents == null) inboundEvents = new ArrayList<>(1024);
            try {
                int count = readBuffer.getInt();
                LOGGER.log(DEBUG,me.identifier + ": Batch of [" + count + "] events received from " + node.identifier);
                TransactionEvent.Payload payload;
                int i = 0;
                while (i < count) {
                    payload = TransactionEvent.read(readBuffer);
                    LOGGER.log(DEBUG, me.identifier+": Processed TID "+payload.tid());
                    if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                        InboundEvent inboundEvent = buildInboundEvent(payload);
                        inboundEvents.add(inboundEvent);
                    }
                    i++;
                }
                if(count != inboundEvents.size()){
                    LOGGER.log(WARNING,me.identifier + ": Batch of [" +count+ "] events != from "+inboundEvents.size()+" that will be pushed to worker " + node.identifier);
                }
                while(!inboundEvents.isEmpty()){
                    if(vmsInternalChannels.transactionInputQueue().offer(inboundEvents.get(0))){
                        inboundEvents.remove(0);
                    }
                }

                LOGGER.log(DEBUG, "Number of inputs pending processing: "+vmsInternalChannels.transactionInputQueue().size());

            } catch(Exception e){
                if (e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading batch: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            } finally {
                inboundEvents.clear();
                LIST_BUFFER.add(inboundEvents);
            }
        }

        @Override
        public void failed(Throwable exc, Integer carryOn) {
            LOGGER.log(ERROR,me.identifier+": Error on reading VMS message from "+node.identifier+"\n"+exc);
            exc.printStackTrace(System.out);
            this.setUpNewRead();
        }
    }

    /**
     * On a connection attempt, it is unknown what is the type of node
     * attempting the connection. We find out after the first read.
     */
    private final class UnknownNodeReadCompletionHandler implements CompletionHandler<Integer, Void> {

        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;

        public UnknownNodeReadCompletionHandler(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            this.channel = channel;
            this.buffer = buffer;
        }

        @Override
        public void completed(Integer result, Void void_) {
            if(result == 0){
                LOGGER.log(WARNING,me.identifier+": A node is trying to connect with an empty message!");
                try { this.channel.close(); } catch (IOException ignored) {}
                return;
            } else if(result == -1){
                LOGGER.log(WARNING,me.identifier+": A node died before sending the presentation message");
                try { this.channel.close(); } catch (IOException ignored) {}
                return;
            }
            // message identifier
            byte messageIdentifier = this.buffer.get(0);
            if(messageIdentifier != PRESENTATION){
                buffer.flip();
                String request = StandardCharsets.UTF_8.decode(this.buffer).toString();
                if(this.isHttpClient(request)){
                    var readCompletionHandler = new HttpReadCompletionHandler(
                            new ConnectionMetadata("http_client".hashCode(),
                                    ConnectionMetadata.NodeType.HTTP_CLIENT,
                                    this.channel),
                            this.buffer);
                    try { NetworkUtils.configure(this.channel, options.osBufferSize()); } catch (IOException ignored) { }
                    readCompletionHandler.process(request);
                } else {
                    LOGGER.log(WARNING, me.identifier + ": A node is trying to connect without a presentation message. \n" + request);
                    this.buffer.clear();
                    MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
                    try { this.channel.close(); } catch (IOException ignored) { }
                }
                return;
            }
            byte nodeTypeIdentifier = this.buffer.get(1);
            this.buffer.position(2);
            switch (nodeTypeIdentifier) {
                case (Presentation.SERVER_TYPE) -> this.processServerPresentation();
                case (Presentation.VMS_TYPE) -> this.processVmsPresentation();
                default -> this.processUnknownNodeType(nodeTypeIdentifier);
            }
        }

        // POST, PATCH, GET
        private boolean isHttpClient(String request) {
            var substr = request.substring(0, request.indexOf(' '));
            switch (substr){
                case "GET", "PATCH", "POST" -> {
                    return true;
                }
            }
            return false;
        }

        private void processServerPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing presentation message from a node claiming to be a server");
            if(!leader.isActive()) {
                ConnectionFromLeaderProtocol connectionFromLeader = new ConnectionFromLeaderProtocol(this.channel, this.buffer);
                connectionFromLeader.processLeaderPresentation();
            } else {
                // discard include metadata bit
                this.buffer.get();
                ServerNode serverNode = Presentation.readServer(this.buffer);
                // known leader attempting additional connection?
                if(serverNode.asInetSocketAddress().equals(leader.asInetSocketAddress())) {
                    LOGGER.log(INFO, me.identifier + ": Leader requested an additional connection");
                    this.buffer.clear();
                    channel.read(buffer, 0, new LeaderReadCompletionHandler(new ConnectionMetadata(leader.hashCode(), ConnectionMetadata.NodeType.SERVER, channel), buffer));
                } else {
                    try {
                        LOGGER.log(WARNING,"Dropping a connection attempt from a node claiming to be leader");
                         this.channel.close();
                    } catch (Exception ignored) {}
                }
            }
        }

        private void processVmsPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing presentation message from a node claiming to be a VMS");

            // then it is a VMS intending to connect due to a data/event
            // that should be delivered to this vms
            VmsNode producerVms = Presentation.readVms(this.buffer, serdesProxy);
            LOGGER.log(INFO, me.identifier+": Producer VMS received:\n"+producerVms);
            this.buffer.clear();

            ConnectionMetadata connMetadata = new ConnectionMetadata(
                    producerVms.hashCode(),
                    ConnectionMetadata.NodeType.VMS,
                    this.channel
            );

            // what if a vms is both producer to and consumer from this vms?
            if(consumerVmsContainerMap.containsKey(producerVms)){
                LOGGER.log(WARNING,me.identifier+": The node "+producerVms.host+" "+producerVms.port+" already contains a connection as a consumer");
            }

            // just to keep track whether this
            if(producerConnectionMetadataMap.containsKey(producerVms.hashCode())) {
                LOGGER.log(INFO, me.identifier+": Setting up additional consumption from producer "+producerVms);
            } else {
                producerConnectionMetadataMap.put(producerVms.hashCode(), connMetadata);
                // setup event receiving from this vms
                LOGGER.log(INFO,me.identifier+": Setting up consumption from producer "+producerVms);
            }

            this.channel.read(this.buffer, 0, new VmsReadCompletionHandler(producerVms, connMetadata, this.buffer));
        }

        private void processUnknownNodeType(byte nodeTypeIdentifier) {
            LOGGER.log(WARNING,me.identifier+": Presentation message from unknown source:" + nodeTypeIdentifier);
            this.buffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
            try {
                this.channel.close();
            } catch (IOException ignored) { }
        }

        @Override
        public void failed(Throwable exc, Void void_) {
            LOGGER.log(WARNING,"Error on processing presentation message!");
        }

    }

    /**
     * Class is iteratively called by the socket pool threads.
     */
    private final class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {
            LOGGER.log(DEBUG,me.identifier+": An unknown host has started a connection attempt.");
            final ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize);
            try {
                NetworkUtils.configure(channel, options.osBufferSize);
                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                channel.read(buffer, null, new UnknownNodeReadCompletionHandler(channel, buffer));
            } catch(Exception e){
                LOGGER.log(ERROR,me.identifier+": Accept handler caught exception: "+e.getMessage());
                buffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(buffer);
            } finally {
                LOGGER.log(DEBUG,me.identifier+": Accept handler set up again for listening to new connections");
                // continue listening
                serverSocket.accept(null, this);
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            String message = exc.getMessage();
            if(message == null){
                if (exc.getCause() instanceof ClosedChannelException){
                    message = "Connection is closed";
                } else if ( exc instanceof AsynchronousCloseException || exc.getCause() instanceof AsynchronousCloseException) {
                    message = "Event handler has been stopped?";
                } else {
                    message = "No cause identified";
                }
            }

            LOGGER.log(WARNING,me.identifier+": Error on accepting connection: "+ message);
            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            } else {
                LOGGER.log(WARNING,me.identifier+": Socket is not open anymore. Cannot set up accept again");
            }
        }
    }

    private final class ConnectionFromLeaderProtocol {
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

        private final class WriteCompletionHandler implements CompletionHandler<Integer,Void> {

            @Override
            public void completed(Integer result, Void attachment) {
                state = State.PRESENTATION_SENT;
                LOGGER.log(INFO,me.identifier+": Message sent to Leader successfully = "+state);
                // set up leader worker
                leaderWorker = new LeaderWorker(me, leader,
                        leaderConnectionMetadata.channel,
                        MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize));
                LOGGER.log(INFO,me.identifier+": Leader worker set up");
                buffer.clear();
                channel.read(buffer, 0, new LeaderReadCompletionHandler(leaderConnectionMetadata, buffer) );
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                LOGGER.log(INFO,me.identifier+": Failed to send presentation to Leader");
                buffer.clear();
                if(!channel.isOpen()) {
                    leaderWorker.stop();
                    leader.off();
                }
                // else what to do try again? no, let the new leader connect
            }
        }

        public void processLeaderPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing the Leader presentation");
            boolean includeMetadata = this.buffer.get() == Presentation.YES;

            // leader has disconnected, or new leader
            leader = Presentation.readServer(this.buffer);

            // read queues leader is interested
            boolean hasQueuesToSubscribe = this.buffer.get() == Presentation.YES;
            if(hasQueuesToSubscribe){
                queuesLeaderSubscribesTo.addAll(Presentation.readQueuesToSubscribeTo(this.buffer, serdesProxy));
            }

            // only connects to all VMSs on first leader connection
            if(leaderConnectionMetadata != null) {
                // considering the leader has replicated the metadata before failing
                // so no need to send metadata again. but it may be necessary...
                // what if the tid and batch id is necessary. the replica may not be
                // sync with last leader...
                LOGGER.log(WARNING, me.identifier+": Updating leader connection metadata due to new connection");
            }
            leaderConnectionMetadata = new ConnectionMetadata(
                    leader.hashCode(),
                    ConnectionMetadata.NodeType.SERVER,
                    channel
            );
            leader.on();
            this.buffer.clear();
            if(includeMetadata) {
                String vmsDataSchemaStr = serdesProxy.serializeDataSchema(me.dataSchema);
                String vmsInputEventSchemaStr = serdesProxy.serializeEventSchema(me.inputEventSchema);
                String vmsOutputEventSchemaStr = serdesProxy.serializeEventSchema(me.outputEventSchema);

                Presentation.writeVms(this.buffer, me, me.identifier, me.batch, 0, me.previousBatch, vmsDataSchemaStr, vmsInputEventSchemaStr, vmsOutputEventSchemaStr);
                // the protocol requires the leader to wait for the metadata in order to start sending messages
            } else {
                Presentation.writeVms(this.buffer, me, me.identifier, me.batch, 0, me.previousBatch);
            }

            this.buffer.flip();
            this.state = State.PRESENTATION_PROCESSED;
            LOGGER.log(INFO,me.identifier+": Message successfully received from the Leader  = "+state);
            this.channel.write( this.buffer, null, this.writeCompletionHandler );
        }
    }

    private InboundEvent buildInboundEvent(TransactionEvent.Payload payload){
        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(payload.event());
        Object input = this.serdesProxy.deserialize(payload.payload(), clazz);
        Map<String, Long> precedenceMap = this.serdesProxy.deserializeDependenceMap(payload.precedenceMap());
        if(precedenceMap == null){
            throw new IllegalStateException("Precedence map is null.");
        }
        if(!precedenceMap.containsKey(this.me.identifier)){
            throw new IllegalStateException("Precedent tid of "+payload.tid()+" is unknown.");
        }
        this.tidToPrecedenceMap.put(payload.tid(), precedenceMap);
        return new InboundEvent( payload.tid(), precedenceMap.get(this.me.identifier),
                payload.batch(), payload.event(), clazz, input );
    }

    private static final ConcurrentLinkedDeque<List<InboundEvent>> LIST_BUFFER = new ConcurrentLinkedDeque<>();

    private final class LeaderReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;

        public LeaderReadCompletionHandler(ConnectionMetadata connectionMetadata, ByteBuffer readBuffer){
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = readBuffer;
            LIST_BUFFER.add(new ArrayList<>(1024));
        }

        @Override
        public void completed(Integer result, Integer startPos) {
            if(result == -1){
                LOGGER.log(INFO,me.identifier+": Leader has disconnected");
                leader.off();
                try {
                    this.connectionMetadata.channel.close();
                } catch (IOException e) {
                    e.printStackTrace(System.out);
                }
                return;
            }
            if(startPos == 0){
                // sets the position to 0 and sets the limit to the current position
                this.readBuffer.flip();
                LOGGER.log(DEBUG,me.identifier+": Leader has sent "+readBuffer.limit()+" bytes");
            }

            // guaranteed we always have at least one byte to read
            byte messageType = this.readBuffer.get();

            try {
                switch (messageType) {
                    //noinspection DuplicatedCode
                    case (BATCH_OF_EVENTS) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        this.processBatchOfEvents(this.readBuffer);
                    }
                    case (EVENT) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        this.processSingleEvent(readBuffer);
                    }
                    case (BATCH_COMMIT_INFO) -> {
                        if(this.readBuffer.remaining() < (BatchCommitInfo.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        // events of this batch from VMSs may arrive before the batch commit info
                        // it means this VMS is a terminal node for the batch
                        BatchCommitInfo.Payload bPayload = BatchCommitInfo.read(this.readBuffer);
                        LOGGER.log(INFO, me.identifier + ": Batch (" + bPayload.batch() + ") commit info received from the leader");
                        this.processNewBatchInfo(bPayload);
                    }
                    case (BATCH_COMMIT_COMMAND) -> {
                        if(this.readBuffer.remaining() < (BatchCommitCommand.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        // a batch commit queue from next batch can arrive before this vms moves next? yes
                        BatchCommitCommand.Payload payload = BatchCommitCommand.read(this.readBuffer);
                        LOGGER.log(DEBUG, me.identifier + ": Batch (" + payload.batch() + ") commit command received from the leader");
                        this.processNewBatchCommand(payload);
                    }
                    case (TX_ABORT) -> {
                        if(this.readBuffer.remaining() < (TransactionAbort.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        TransactionAbort.Payload txAbortPayload = TransactionAbort.read(this.readBuffer);
                        LOGGER.log(WARNING, "Transaction (" + txAbortPayload.batch() + ") abort received from the leader?");
                        vmsInternalChannels.transactionAbortInputQueue().add(txAbortPayload);
                    }
                    case (BATCH_ABORT_REQUEST) -> {
                        if(this.readBuffer.remaining() < (BatchAbortRequest.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        // some new leader request to roll back to last batch commit
                        BatchAbortRequest.Payload batchAbortReq = BatchAbortRequest.read(this.readBuffer);
                        LOGGER.log(WARNING, "Batch (" + batchAbortReq.batch() + ") abort received from the leader");
                        // vmsInternalChannels.batchAbortQueue().add(batchAbortReq);
                    }
                    case (CONSUMER_SET) -> {
                        try {
                            LOGGER.log(INFO, me.identifier + ": Consumer set received from the leader");
                            Map<String, List<IdentifiableNode>> receivedConsumerVms = ConsumerSet.read(this.readBuffer, serdesProxy);
                            if (!receivedConsumerVms.isEmpty()) {
                                connectToReceivedConsumerSet(receivedConsumerVms);
                            } else {
                                LOGGER.log(WARNING, me.identifier + ": Consumer set is empty");
                            }
                        } catch (IOException e) {
                            LOGGER.log(ERROR, me.identifier + ": IOException while reading consumer set: " + e);
                            e.printStackTrace(System.out);
                        }
                    }
                    case (PRESENTATION) ->
                            LOGGER.log(WARNING, me.identifier + ": Presentation being sent again by the leader!?");
                    default ->
                            LOGGER.log(ERROR, me.identifier + ": Message type sent by the leader cannot be identified: " + messageType);
                }
            } catch (Exception e){
                LOGGER.log(ERROR, "Leader: Error caught\n"+e.getMessage(), e);
                e.printStackTrace(System.out);
            }

            if(this.readBuffer.hasRemaining()){
                this.completed(result, this.readBuffer.position());
            } else {
                this.setUpNewRead();
            }
        }

        private int getBufferSize() {
            int bufferSize = Integer.MAX_VALUE;
            // check if we can read an integer
            if(this.readBuffer.remaining() > Integer.BYTES) {
                // size of the batch
                bufferSize = this.readBuffer.getInt();
                // discard message type and size of batch from the total size since it has already been read
                bufferSize -= 1 + Integer.BYTES;
            }
            return bufferSize;
        }

        /**
         * This method should be called only when strictly necessary to complete a read
         * Otherwise there would be an overhead due to the many I/Os
         */
        private void fetchMoreBytes(Integer startPos) {
            this.readBuffer.position(startPos);
            this.readBuffer.compact();
            // get the rest of the batch
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void setUpNewRead() {
            this.readBuffer.clear();
            // set up another read for cases of bursts of data
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        @SuppressWarnings("SequencedCollectionMethodCanBeUsed")
        private void processBatchOfEvents(ByteBuffer readBuffer) {
            List<InboundEvent> payloads = LIST_BUFFER.poll();
            if(payloads == null) payloads = new ArrayList<>(1024);
            /*
             * Given a new batch of events sent by the leader, the last message is the batch info
             */
            TransactionEvent.Payload payload;
            try {
                // to increase performance, one would buffer this buffer for processing and then read from another buffer
                int count = readBuffer.getInt();
                LOGGER.log(DEBUG,me.identifier + ": Batch of [" + count + "] events received from the leader");

                // extract events batched
                for (int i = 0; i < count; i++) {
                    payload = TransactionEvent.read(readBuffer);
                    LOGGER.log(DEBUG, me.identifier+": Processed TID "+payload.tid());
                    if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                        payloads.add(buildInboundEvent(payload));
                    }
                }

                // add after to make sure the batch context map is filled by the time the output event is generated
                while(!payloads.isEmpty()){
                    if(vmsInternalChannels.transactionInputQueue().offer(payloads.get(0))){
                        payloads.remove(0);
                    }
                }

            } catch (Exception e){
                LOGGER.log(ERROR, me.identifier +": Error while processing a batch\n"+e);
                e.printStackTrace(System.out);
                if(e instanceof BufferUnderflowException) {
                    throw new RuntimeException(e);
                }
            } finally {
                payloads.clear();
                LIST_BUFFER.add(payloads);
            }
        }

        private void processSingleEvent(ByteBuffer readBuffer) {
            try {
                TransactionEvent.Payload payload = TransactionEvent.read(readBuffer);
                LOGGER.log(DEBUG,me.identifier + ": 1 event received from the leader \n"+payload);
                // send to scheduler.... drop if the event cannot be processed (not an input event in this vms)
                if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                    InboundEvent event = buildInboundEvent(payload);
                    vmsInternalChannels.transactionInputQueue().add(event);
                }
            } catch (Exception e) {
                if(e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading event: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            }
        }

        private void processNewBatchInfo(BatchCommitInfo.Payload batchCommitInfo){
            BatchContext batchContext = BatchContext.build(batchCommitInfo);
            batchContextMap.put(batchCommitInfo.batch(), batchContext);
            batchToNextBatchMap.put( batchCommitInfo.previousBatch(), batchCommitInfo.batch() );
        }

        /**
         * Context of execution of this method:
         * This is not a terminal node in this batch, which means
         * it does not know anything about the batch commit command just received.
         * If the previous batch is completed and this received batch is the next,
         * we just let the main loop update it
         */
        private void processNewBatchCommand(BatchCommitCommand.Payload batchCommitCommand){
            BatchContext batchContext = BatchContext.build(batchCommitCommand);
            batchContextMap.put(batchCommitCommand.batch(), batchContext);
            batchToNextBatchMap.put( batchCommitCommand.previousBatch(), batchCommitCommand.batch() );
        }

        @Override
        public void failed(Throwable exc, Integer carryOn) {
            LOGGER.log(ERROR,me.identifier+": Message could not be processed: "+exc);
            exc.printStackTrace(System.out);
            this.setUpNewRead();
        }
    }

}
