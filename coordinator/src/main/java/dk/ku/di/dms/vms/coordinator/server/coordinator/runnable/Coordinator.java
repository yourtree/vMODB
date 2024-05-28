package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchAlgo;
import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.follower.BatchReplication;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy.ALL;
import static dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.PRESENTATION;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.SERVER_TYPE;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.SERVER;
import static java.lang.System.Logger.Level.*;

/**
 * Also known as the "Leader"
 * Class that encapsulates all logic related to issuing of
 * transactions, batch commits, transaction aborts, ...
 */
public final class Coordinator extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(Coordinator.class.getName());
    
    private final CoordinatorOptions options;

    // this server socket
    private final AsynchronousServerSocketChannel serverSocket;

    // group for channels
    private final AsynchronousChannelGroup group;

    // general tasks, like sending info to VMSs and other servers
    // private final ExecutorService taskExecutor;

    // even though we can start with a known number of servers, their payload may have changed after a crash
    private final Map<Integer, ServerNode> servers;

    // for server nodes
    private final Map<Integer, LockConnectionMetadata> serverConnectionMetadataMap;

    /* VMS data structures **/

    /**
     * received from program start
     * also called known VMSs
     */
    private final Map<Integer, IdentifiableNode> starterVMSs;

    /**
     * Those received from program start + those that joined later
     * shared with vms workers
     */
    private final Map<String, VmsNode> vmsMetadataMap;

    // the identification of this server
    private final ServerNode me;

    // must update the "me" on snapshotting (i.e., committing)
    private long tid;

    /**
     * the current batch on which new transactions are being generated
     * for optimistic generation of batches (non-blocking on commit)
     * this value may be way ahead of batchOffsetPendingCommit
     */
    private long currentBatchOffset;

    /*
     * the offset of the pending batch commit (always < batchOffset)
     * volatile because it is accessed by vms workers
     */
    private long batchOffsetPendingCommit;

    // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    // transaction requests coming from the http event loop
    private final List<ConcurrentLinkedDeque<TransactionInput>> transactionInputDeques;

    // transaction definitions coming from the http event loop
    private final Map<String, TransactionDAG> transactionMap;

    /** serialization and deserialization of complex objects **/
    private final IVmsSerdesProxy serdesProxy;

    private final int networkBufferSize;

    private final int osBufferSize;

    private final int networkSendTimeout;

    private final BlockingQueue<Long> batchSignalQueue;

    private final BlockingQueue<Object> coordinatorQueue;

    public static Coordinator build(// obtained from leader election or passed by parameter on setup
                                    Map<Integer, ServerNode> servers,
                                    // passed by parameter
                                    Map<Integer, IdentifiableNode> startersVMSs,
                                    Map<String, TransactionDAG> transactionMap,
                                    ServerNode me,
                                    // coordinator configuration
                                    CoordinatorOptions options,
                                    // starting batch offset (may come from storage after a crash)
                                    long startingBatchOffset,
                                    // starting tid (may come from storage after a crash)
                                    long startingTid,
                                    IVmsSerdesProxy serdesProxy) throws IOException {
        return new Coordinator(servers == null ? new ConcurrentHashMap<>() : servers,
                new HashMap<>(), startersVMSs, Objects.requireNonNull(transactionMap),
                me, options, startingBatchOffset, startingTid, serdesProxy);
    }

    private Coordinator(Map<Integer, ServerNode> servers,
                        Map<Integer, LockConnectionMetadata> serverConnectionMetadataMap,
                        Map<Integer, IdentifiableNode> startersVMSs,
                        Map<String, TransactionDAG> transactionMap,
                        ServerNode me,
                        CoordinatorOptions options,
                        long startingBatchOffset,
                        long startingTid,
                        IVmsSerdesProxy serdesProxy) throws IOException {
        super();

        // coordinator options
        this.options = options;

        if(options.getGroupThreadPoolSize() > 0) {
            this.group = AsynchronousChannelGroup.withThreadPool(Executors.newWorkStealingPool(options.getGroupThreadPoolSize()));
            this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        } else {
            this.group = null;
            this.serverSocket = AsynchronousServerSocketChannel.open();
        }

        // network and executor
        this.serverSocket.bind(me.asInetSocketAddress());

        this.starterVMSs = startersVMSs;
        this.vmsMetadataMap = new ConcurrentHashMap<>();

        // might come filled from election process
        this.servers = servers;
        this.serverConnectionMetadataMap = serverConnectionMetadataMap;
        this.me = me;

        // infra
        this.serdesProxy = serdesProxy;

        // shared data structure with http handler
        this.transactionInputDeques = new ArrayList<>();
        for(int i = 0; i < options.getTaskThreadPoolSize(); i++){
            this.transactionInputDeques.add(new ConcurrentLinkedDeque<>());
        }

        // in production, it requires receiving new transaction definitions
        this.transactionMap = transactionMap;

        // to hold actions spawned by events received by different VMSs
        this.coordinatorQueue = new LinkedBlockingQueue<>();

        // batch commit metadata
        this.currentBatchOffset = startingBatchOffset;
        this.batchOffsetPendingCommit = startingBatchOffset;
        this.tid = startingTid;

        // initialize batch offset map
        this.batchContextMap = new HashMap<>();
        this.batchContextMap.put(this.currentBatchOffset, new BatchContext(this.currentBatchOffset));
        BatchContext previousBatch = new BatchContext(this.currentBatchOffset - 1);
        previousBatch.seal(startingTid - 1, Map.of(), Map.of());
        this.batchContextMap.put(this.currentBatchOffset - 1, previousBatch);

        if(options.getNetworkBufferSize() > 0)
            this.networkBufferSize = options.getNetworkBufferSize();
        else
            this.networkBufferSize = MemoryUtils.DEFAULT_PAGE_SIZE;

        this.networkSendTimeout = options.getNetworkSendTimeout();
        this.osBufferSize = options.getOsBufferSize();

        this.batchSignalQueue = new LinkedBlockingQueue<>();
    }

    private final Map<String, List<VmsNode>> vmsIdentifiersPerDAG = new HashMap<>();

    /**
     * This method contains the event loop that contains the main functions of a leader/coordinator
     * What happens if two nodes declare themselves as leaders? We need some way to let it know
     * OK (b) Batch management
     * designing leader mode first
     * design follower mode in another class to avoid convoluted code.
     * Going for a different socket to allow for heterogeneous ways for a client to connect with the servers e.g., http.
     * It is also good to better separate resources, so VMSs and followers do not share resources with external clients
     */
    @Override
    public void run() {

        // setup asynchronous listener for new connections
        this.serverSocket.accept(null, new AcceptCompletionHandler());
        // connect to all virtual microservices
        this.setupStarterVMSs();

        Thread.ofPlatform().factory().newThread(this::processEventsSentByVmsWorkers).start();

        LOGGER.log(DEBUG,"Leader: Waiting for all starter VMSs to connect");
        // process all VMS_IDENTIFIER first before submitting transactions
        for(;;){
            if(this.vmsMetadataMap.size() >= this.starterVMSs.size())
                break;
        }

        int taskThreadPoolSize = this.options.getTaskThreadPoolSize();
        LOGGER.log(DEBUG,"Leader: Preprocessing transaction DAGs");
        Set<String> inputVMSsSeen = new HashSet<>();
        // build list of VmsIdentifier per transaction DAG
        for(var dag : this.transactionMap.entrySet()){
            this.vmsIdentifiersPerDAG.put(dag.getKey(), BatchAlgo.buildTransactionDagVmsList( dag.getValue(), this.vmsMetadataMap ));

            // set up additional connection with vms that start transactions
            for(var inputVms : dag.getValue().inputEvents.entrySet()){
                var vmsNode = this.vmsMetadataMap.get(inputVms.getValue().targetVms);
                if(inputVMSsSeen.contains(vmsNode.identifier)){
                    continue;
                }
                inputVMSsSeen.add(vmsNode.identifier);
                for(int i = 1; i < taskThreadPoolSize; i++){
                    if(this.vmsWorkerContainer.containsKey(inputVms.getValue().targetVms)) {
                        VmsWorker newWorker = VmsWorker.build(this.me, vmsNode, this.coordinatorQueue,
                                this.group, this.networkBufferSize, this.networkSendTimeout, this.serdesProxy);
                        this.vmsWorkerContainer.get(inputVms.getValue().targetVms).addWorker(newWorker);
                        Thread vmsWorkerThread = Thread.ofPlatform().factory().newThread(newWorker);
                        vmsWorkerThread.setName("vms-worker-" + vmsNode.identifier + "-" + (i));
                        vmsWorkerThread.start();
                    }
                }
            }
        }

        LOGGER.log(DEBUG,"Leader: Transaction processing starting now... ");
        long start = System.currentTimeMillis();
        long end = start + this.options.getBatchWindow();

        int dequeIdx = 0;
        ConcurrentLinkedDeque<TransactionInput> deque;
        TransactionInput data;
        while(this.isRunning()){
            do {
                // iterate over deques and drain transaction inputs
                do {
                    deque = this.transactionInputDeques.get(dequeIdx);
                    while ((data = deque.poll()) != null) {
                        this.transactionRequests.add(data);
                    }
                    dequeIdx++;
                } while (dequeIdx < taskThreadPoolSize && System.currentTimeMillis() < end);
                dequeIdx = 0;
                this.processTransactionInputEvents();
            } while(System.currentTimeMillis() < end);
            this.advanceCurrentBatch();
            start = System.currentTimeMillis();
            end = start + this.options.getBatchWindow();
        }

        this.failSafeClose();
        LOGGER.log(INFO,"Leader: Finished execution.");
    }

    /**
     * A container of vms workers for the same VMS
     * Make a circular buffer. so events are spread among the workers (i.e., channels)
     */
    private static final class VmsWorkerContainer {

        private final List<VmsWorker> vmsWorkers;
        private int next;

        VmsWorkerContainer(VmsWorker initialVmsWorker) {
            this.vmsWorkers = new ArrayList<>();
            this.vmsWorkers.add(initialVmsWorker);
            this.next = 0;
        }

        public void addWorker(VmsWorker vmsWorker) {
            this.vmsWorkers.add(vmsWorker);
        }

        @SuppressWarnings("SequencedCollectionMethodCanBeUsed")
        public void queue(Object object){

            if(!(object instanceof TransactionEvent.PayloadRaw)){
                // queue to first workers.
                this.vmsWorkers.get(0).queueMessage(object);
                return;
            }
            this.vmsWorkers.get(this.next).queueMessage(object);

            if(this.next == this.vmsWorkers.size()-1){
                this.next = 0;
            } else {
                this.next += 1;
            }
        }
    }

    private final Map<String, VmsWorkerContainer> vmsWorkerContainer = new HashMap<>();

    /**
     * After a leader election, it makes more sense that
     * the leader connects to all known virtual microservices.
     * No need to track the thread created because they will be
     * later mapped to the respective vms identifier by the thread
     */
    private void setupStarterVMSs() {
        for(IdentifiableNode vmsNode : this.starterVMSs.values()){
            // coordinator will later keep track of this thread when the connection with the VMS is fully established
            final VmsWorker vmsWorker = VmsWorker.buildAsStarter(this.me, vmsNode, this.coordinatorQueue,
                    this.group, this.networkBufferSize, this.networkSendTimeout, this.serdesProxy);
            // a cached thread pool would be ok in this case
            Thread vmsWorkerThread = Thread.ofPlatform().factory().newThread(vmsWorker);
            vmsWorkerThread.setName("vms-worker-"+vmsNode.identifier+"-0");
            vmsWorkerThread.start();
            this.vmsWorkerContainer.put(vmsNode.identifier,
                    new VmsWorkerContainer(vmsWorker)
            );
        }
    }

    /**
     * Match output of a vms with the input of another
     * for each vms input event (not generated by the coordinator),
     * find the vms that generated the output
     */
    private List<IdentifiableNode> findConsumerVMSs(String outputEvent){
        List<IdentifiableNode> list = new ArrayList<>(2);
        // can be the leader or a vms
        for( VmsNode vms : this.vmsMetadataMap.values() ){
            if(vms.inputEventSchema.get(outputEvent) != null){
                list.add(vms);
            }
        }
        // assumed to be terminal? maybe yes.
        // vms is already connected to leader, no need to return coordinator
        return list;
    }

    private void failSafeClose(){
        // safe close
        try { this.serverSocket.close(); } catch (IOException ignored) {}
    }

    private final Random random = new Random();

    /**
     * Distribute the inserts across many deques to avoid contention across http threads
     */
    public boolean queueTransactionInput(TransactionInput transactionInput) {
        int idx = this.random.nextInt(0, options.getTaskThreadPoolSize());
        return this.transactionInputDeques.get(idx).offerLast(transactionInput);
    }

    /**
     * This is where I define whether the connection must be kept alive
     * Depending on the nature of the request:
     * <a href="https://www.baeldung.com/java-nio2-async-socket-channel">...</a>
     * The first read must be a presentation message, informing what is this server (follower or VMS)
     */
    private final class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {

            ByteBuffer buffer = null;

            try {
                NetworkUtils.configure(channel, osBufferSize);

                // right now I cannot discern whether it is a VMS or follower. perhaps I can keep alive channels from leader election?
                buffer = MemoryManager.getTemporaryDirectBuffer(networkBufferSize);

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                // this will be handled by another thread in the group
                channel.read(buffer, buffer, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, ByteBuffer buffer) {
                        // set this thread free. release the thread that belongs to the channel group
                        processReadAfterAcceptConnection(channel, buffer);
                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer buffer) {
                        MemoryManager.releaseTemporaryDirectBuffer(buffer);
                    }
                });


            } catch (Exception e) {
                LOGGER.log(WARNING,"Leader: Error on accepting connection on " + me);
                if (buffer != null) {
                    MemoryManager.releaseTemporaryDirectBuffer(buffer);
                }
            } finally {
                // continue listening
                if (serverSocket.isOpen()) {
                    serverSocket.accept(null, this);
                }
            }

        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (serverSocket.isOpen()) {
                serverSocket.accept(null, this);
            }
        }

        /**
         *
         * Process Accept connection request
         * Task for informing the server running for leader that a leader is already established
         * We would no longer need to establish connection in case the {@link dk.ku.di.dms.vms.coordinator.election.ElectionWorker}
         * maintains the connections.
         */
        private void processReadAfterAcceptConnection(AsynchronousSocketChannel channel, ByteBuffer buffer){

            // message identifier
            byte messageIdentifier = buffer.get(0);

            if(messageIdentifier == VOTE_REQUEST || messageIdentifier == VOTE_RESPONSE){
                // so I am leader, and I respond with a leader request to this new node
                // taskExecutor.submit( new ElectionWorker.WriteTask( LEADER_REQUEST, server ) );
                // would be better to maintain the connection open.....
                buffer.clear();

                if(channel.isOpen()) {
                    LeaderRequest.write(buffer, me);
                    buffer.flip();
                    try (channel) {
                        channel.write(buffer); // write and forget
                    } catch (IOException ignored) {
                    } finally {
                        MemoryManager.releaseTemporaryDirectBuffer(buffer);
                    }
                }
                return;
            }

            if(messageIdentifier == LEADER_REQUEST){
                // buggy node intending to pose as leader...
                try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch(Exception ignored){}
                return;
            }

            // if it is not a presentation, drop connection
            if(messageIdentifier != PRESENTATION){
                LOGGER.log(WARNING,"A node is trying to connect without a presentation message:");
                try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch(Exception ignored){}
                return;
            }

            // now let's do the work
            buffer.position(1);

            byte type = buffer.get();
            if(type == SERVER_TYPE){
                this.processServerPresentationMessage(channel, buffer);
            } else {
                // simply unknown... probably a bug?
                LOGGER.log(WARNING,"Unknown type of client connection. Probably a bug? ");
                try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch(Exception ignored){}
            }
        }

        /**
         * Still need to define what to do with connections from replicas....
         */
        private void processServerPresentationMessage(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            // server
            // ....
            ServerNode newServer = Presentation.readServer(buffer);

            // check whether this server is known... maybe it has crashed... then we only need to update the respective channel
            if(servers.get(newServer.hashCode()) != null){
                LockConnectionMetadata connectionMetadata = serverConnectionMetadataMap.get( newServer.hashCode() );
                // update metadata of this node
                servers.put( newServer.hashCode(), newServer );
                connectionMetadata.channel = channel;
            } else { // no need for locking here
                servers.put( newServer.hashCode(), newServer );
                LockConnectionMetadata connectionMetadata = new LockConnectionMetadata(
                        newServer.hashCode(), SERVER,
                        buffer,
                        MemoryManager.getTemporaryDirectBuffer(networkBufferSize),
                        channel,
                        new Semaphore(1) );
                serverConnectionMetadataMap.put( newServer.hashCode(), connectionMetadata );
            }
        }
    }

    /**
     * Given a set of VMSs involved in the last batch
     * (for easiness can send to all of them for now)
     * send a batch request.
     * For logging, must store the transaction inputs in disk before sending
     */
    private void advanceCurrentBatch(){

        BatchContext previousBatch = this.batchContextMap.get( this.currentBatchOffset - 1 );
        // have we processed any input event since the start of this coordinator thread?
        if(previousBatch.lastTid == this.tid - 1){
            LOGGER.log(DEBUG,"Leader: No new transaction since last batch generation. Current batch "+this.currentBatchOffset+" won't be spawned this time.");
            return;
        }

        // why do I need to replicate vmsTidMap? to restart from this point if the leader fails
        final long generateBatch = this.currentBatchOffset;

        LOGGER.log(INFO,"Leader: Batch commit task for batch offset "+generateBatch+" is starting...");

        BatchContext currBatchContext = this.batchContextMap.get( generateBatch );

        Map<String,Long> previousBatchPerVms = this.vmsMetadataMap.values().stream()
                .filter(p-> p.batch == generateBatch)
                .collect(Collectors.toMap( VmsNode::getIdentifier, VmsNode::getPreviousBatch) );

        Map<String,Integer> numberOfTasksPerVms = this.vmsMetadataMap.values().stream()
                .filter(p-> p.batch == generateBatch)
                .collect(Collectors.toMap( VmsNode::getIdentifier, VmsNode::getNumberOfTIDs) );

        currBatchContext.seal(this.tid - 1, previousBatchPerVms, numberOfTasksPerVms);

        // increment batch offset
        this.currentBatchOffset = this.currentBatchOffset + 1;

        // define new batch context
        BatchContext newBatchContext = new BatchContext(this.currentBatchOffset);
        this.batchContextMap.put( this.currentBatchOffset, newBatchContext );

        // new TIDs will be emitted with the new batch in the transaction manager
        // just iterate over terminals directly
        for(String terminalVms : currBatchContext.terminalVMSs){
            VmsNode vms = this.vmsMetadataMap.get(terminalVms);
            this.vmsWorkerContainer.get(terminalVms).queue(
                    BatchCommitInfo.of(vms.batch, vms.previousBatch, vms.numberOfTIDsCurrentBatch)
            );
        }

        LOGGER.log(INFO,"Leader: Next batch offset will be "+this.currentBatchOffset);

        // the batch commit only has progress (a safety property) the way it is implemented now when future events
        // touch the same VMSs that have been touched by transactions in the last batch.
        // how can we guarantee progress?
        this.replicateBatchWithReplicas(currBatchContext);
    }

    private void replicateBatchWithReplicas(BatchContext batchContext) {
        if (this.options.getBatchReplicationStrategy() == NONE) return;

        // to refrain the number of servers increasing concurrently, instead of
        // synchronizing the operation, I can simply obtain the collection first
        // but what happens if one of the servers in the list fails?
        Collection<ServerNode> activeServers = this.servers.values();
        int nServers = activeServers.size();

        CompletableFuture<?>[] promises = new CompletableFuture[nServers];

        Set<Integer> serverVotes = Collections.synchronizedSet(new HashSet<>(nServers));

        // String lastTidOfBatchPerVmsJson = this.serdesProxy.serializeMap(batchContext.lastTidOfBatchPerVms);

        int i = 0;
        for (ServerNode server : activeServers) {

            if (!server.isActive()) continue;
            promises[i] = CompletableFuture.supplyAsync(() ->
            {
                // could potentially use another channel for writing commit-related messages...
                // could also just open and close a new connection
                // actually I need this since I must read from this thread instead of relying on the
                // read completion handler
                AsynchronousSocketChannel channel = null;
                try {

                    InetSocketAddress address = new InetSocketAddress(server.host, server.port);
                    channel = AsynchronousSocketChannel.open(group);
                    NetworkUtils.configure(channel, osBufferSize);
                    channel.connect(address).get();

                    ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(networkBufferSize);
                    // BatchReplication.write(buffer, batchContext.batchOffset, lastTidOfBatchPerVmsJson);
                    channel.write(buffer).get();

                    buffer.clear();

                    // immediate read in the same channel
                    channel.read(buffer).get();

                    BatchReplication.Payload response = BatchReplication.read(buffer);

                    buffer.clear();

                    MemoryManager.releaseTemporaryDirectBuffer(buffer);

                    // assuming the follower always accept
                    if (batchContext.batchOffset == response.batch()) serverVotes.add(server.hashCode());

                    return null;

                } catch (InterruptedException | ExecutionException | IOException e) {
                    // cannot connect to host
                    LOGGER.log(WARNING,"Error connecting to host. I am " + me.host + ":" + me.port + " and the target is " + server.host + ":" + server.port);
                    return null;
                } finally {
                    if (channel != null && channel.isOpen()) {
                        try {
                            channel.close();
                        } catch (IOException ignored) {
                        }
                    }
                }

                // these threads need to block to wait for server response

            });
            i++;
        }

        // if none, do nothing
        if ( this.options.getBatchReplicationStrategy() == AT_LEAST_ONE){
            // asynchronous
            // at least one is always necessary
            int j = 0;
            while (j < nServers && serverVotes.isEmpty()){
                promises[i].join();
                j++;
            }
            if(serverVotes.isEmpty()){
                LOGGER.log(WARNING,"The system has entered in a state that data may be lost since there are no followers to replicate the current batch offset.");
            }
        } else if ( this.options.getBatchReplicationStrategy() == MAJORITY ){

            int simpleMajority = ((nServers + 1) / 2);
            // idea is to iterate through servers, "joining" them until we have enough
            int j = 0;
            while (j < nServers && serverVotes.size() <= simpleMajority){
                promises[i].join();
                j++;
            }

            if(serverVotes.size() < simpleMajority){
                LOGGER.log(WARNING,"The system has entered in a state that data may be lost since a majority have not been obtained to replicate the current batch offset.");
            }
        } else if ( this.options.getBatchReplicationStrategy() == ALL ) {
            CompletableFuture.allOf( promises ).join();
            if ( serverVotes.size() < nServers ) {
                LOGGER.log(WARNING,"The system has entered in a state that data may be lost since there are missing votes to replicate the current batch offset.");
            }
        }

        // for now, we don't have a fallback strategy...

    }

    /**
     * Local list of requests to be processed
     * Must be cleaned after use
     */
    private final List<TransactionInput> transactionRequests = new ArrayList<>(100000);

    /**
     * Should read in a proportion that matches the batch and heartbeat window, otherwise
     * how long does it take to process a batch of input transactions?
     * instead of trying to match the rate of processing, perhaps we can create read tasks
     * -
     * processing the transaction input and creating the
     * corresponding events
     * the network buffering will send
     */
    private void processTransactionInputEvents(){

        for(TransactionInput transactionInput : this.transactionRequests){

            TransactionDAG transactionDAG = this.transactionMap.get( transactionInput.name );

//            boolean allKnown = true;
            // first make sure the metadata of this vms is known
            // that ensures that, even if the vms is down, eventually it will come to life
//            for (TransactionInput.Event inputEvent : transactionInput.events) {
//                EventIdentifier event = transactionDAG.inputEvents.get(inputEvent.name);
//                VmsNode vms = this.vmsMetadataMap.get(event.targetVms);
//                if(vms == null) {
//                    logger.log(WARNING,"VMS "+event.targetVms+" is unknown to the coordinator");
//                    allKnown = false;
//                    break;
//                }
//            }

//            if(!allKnown) continue; // got to the next transaction

            // this is the only thread updating this value, so it is by design an atomic operation
            // ++ after variable returns the value before incrementing
            long tid_ = this.tid++;

            // for each input event, send the event to the proper vms
            // assuming the input is correct, i.e., all events are present
            //for (TransactionInput.Event inputEvent : transactionInput.events) {
                // look for the event in the topology
            EventIdentifier event = transactionDAG.inputEvents.get(transactionInput.event.name);

            // get the vms
            VmsNode inputVms = this.vmsMetadataMap.get(event.targetVms);

            // a vms, although receiving an event from a "next" batch, cannot yet commit, since
            // there may have additional events to arrive from the current batch
            // so the batch request must contain the last tid of the given vms

            // if an internal/terminal vms do not receive an input event in this batch, it won't be able to
            // progress since the precedence info will never arrive
            // this way, we need to send in this event the precedence info for all downstream VMSs of this event
            // having this info avoids having to contact all internal/terminal nodes to inform the precedence of events
            List<VmsNode> vmsList = this.vmsIdentifiersPerDAG.get( transactionDAG.name );
            Map<String, Long> previousBatchPerVms = vmsList.stream()
                    .collect(Collectors.toMap( VmsNode::getIdentifier, VmsNode::getLastTidOfBatch) );
            String precedenceMapStr = this.serdesProxy.serializeMap(previousBatchPerVms);

            // write. think about failures/atomicity later
            TransactionEvent.PayloadRaw txEvent = TransactionEvent.of(tid_, this.currentBatchOffset,
                    transactionInput.event.name, transactionInput.event.payload, precedenceMapStr);

            // assign this event, so... what? try to send later? if a vms fail, the last event is useless, we need to send the whole batch generated so far...

            if(!event.targetVms.equalsIgnoreCase(inputVms.identifier)){
                LOGGER.log(ERROR,"Leader: The event was going to be queued to the incorrect VMS worker!");
            }
            LOGGER.log(DEBUG,"Leader: Adding event "+event.name+" to "+inputVms.identifier+" worker");

            this.vmsWorkerContainer.get(inputVms.identifier).queue(txEvent);

            /*
             * update for next transaction. this is basically to ensure VMS do not wait for a tid that will never come. TIDs processed by a vms may not be sequential
             */
            for(var vms_ : vmsList){
                vms_.lastTidOfBatch = tid_;
                this.updateVmsBatchAndPrecedenceIfNecessary(vms_);
                vms_.numberOfTIDsCurrentBatch++;
            }

            // add terminal to the set... so cannot be immutable when the batch context is created...
            this.batchContextMap.get(this.currentBatchOffset).terminalVMSs.addAll( transactionDAG.terminalNodes );

            // update last tid? no, it will be updated on generate batch
            // this.batchContextMap.get(this.currentBatchOffset).lastTid = tid_;
        }

        // clear it, otherwise it will issue duplicate events
        this.transactionRequests.clear();
    }

    private void updateVmsBatchAndPrecedenceIfNecessary(VmsNode vms) {
        if(vms.batch != this.currentBatchOffset){
            vms.previousBatch = vms.batch;
            vms.batch = this.currentBatchOffset;
            vms.numberOfTIDsCurrentBatch = 0;
        }
    }

    private static final boolean BLOCKING = true;

    /**
     * This task assumes the channels are already established
     * Cannot have two threads writing to the same channel at the same time
     * A transaction manager is responsible for assigning TIDs to incoming transaction requests
     * This task also involves making sure the writes are performed successfully
     * A writer manager is responsible for defining strategies, policies, safety guarantees on
     * writing concurrently to channels.
     * <a href="https://web.mit.edu/6.005/www/fa14/classes/20-queues-locks/message-passing/">Message passing in Java</a>
     */
    private void processEventsSentByVmsWorkers() {
        int pollTimeout = 50;
        Object message;
        while(this.isRunning()){
            try {
                if(BLOCKING){
                    message = this.coordinatorQueue.take();
                } else {
                    message = this.coordinatorQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
                    // then put to poll again
                    if (message == null) {
                        pollTimeout = pollTimeout * 2;
                        continue;
                    }
                    // decrease for next polling
                    pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
                }

                switch(message){
                    // receive metadata from all microservices
                    case VmsNode vmsIdentifier_ -> {
                        LOGGER.log(INFO,"Leader: Received a VMS_IDENTIFIER from: "+vmsIdentifier_.identifier);
                        // update metadata of this node so coordinator can reason about data dependencies
                        this.vmsMetadataMap.put( vmsIdentifier_.identifier, vmsIdentifier_ );

                        if(this.vmsMetadataMap.size() < this.starterVMSs.size()) {
                            LOGGER.log(INFO,"Leader: "+(this.starterVMSs.size() - this.vmsMetadataMap.size())+" starter(s) VMSs remain to be processed.");
                            continue;
                        }
                        // if all metadata, from all starter vms have arrived, then send the signal to them

                        LOGGER.log(INFO,"Leader: All VMS starter have sent their VMS_IDENTIFIER");

                        // new VMS may join, requiring updating the consumer set
                        Map<String, List<IdentifiableNode>> vmsConsumerSet;

                        for(VmsNode vmsIdentifier : this.vmsMetadataMap.values()) {

                            // if we reuse the hashmap, the entries get mixed and lead to incorrect consumer set
                            vmsConsumerSet = new HashMap<>();

                            IdentifiableNode vms = this.starterVMSs.get( vmsIdentifier.hashCode() );
                            if(vms == null) {
                                LOGGER.log(WARNING,"Leader: Could not identify "+vmsIdentifier.getIdentifier()+" from set of starter VMSs");
                                continue;
                            }

                            // build global view of vms dependencies/interactions
                            // build consumer set dynamically
                            // for each output event, find the consumer VMSs
                            for (VmsEventSchema eventSchema : vmsIdentifier.outputEventSchema.values()) {
                                List<IdentifiableNode> nodes = this.findConsumerVMSs(eventSchema.eventName);
                                if (!nodes.isEmpty())
                                    vmsConsumerSet.put(eventSchema.eventName, nodes);
                            }

                            String mapStr = "";
                            if (!vmsConsumerSet.isEmpty()) {
                                mapStr = this.serdesProxy.serializeConsumerSet(vmsConsumerSet);
                                LOGGER.log(INFO,"Leader: Consumer set built for "+vmsIdentifier.getIdentifier()+": "+mapStr);
                            }
                            this.vmsWorkerContainer.get(vmsIdentifier.identifier).queue(mapStr);
                        }
                    }
                    case TransactionAbort.Payload msg -> {
                        // send abort to all VMSs...
                        // later we can optimize the number of messages since some VMSs may not need to receive this abort
                        // cannot commit the batch unless the VMS is sure there will be no aborts...
                        // this is guaranteed by design, since the batch complete won't arrive unless all events of the batch arrive at the terminal VMSs
                        this.batchContextMap.get( msg.batch() ).tidAborted = msg.tid();
                        // can reuse the same buffer since the message does not change across VMSs like the commit request
                        for(VmsNode vms : this.vmsMetadataMap.values()){
                            // don't need to send to the vms that aborted
                            // if(vms.getIdentifier().equalsIgnoreCase( msg.vms() )) continue;
                            this.vmsWorkerContainer.get(vms.identifier).queue(msg.tid());
                        }
                    }
                    case BatchComplete.Payload msg -> {
                        // what if ACKs from VMSs take too long? or never arrive?
                        // need to deal with intersecting batches? actually just continue emitting for higher throughput
                        LOGGER.log(DEBUG,"Leader: Processing batch ("+msg.batch()+") complete from: "+msg.vms());
                        BatchContext batchContext = this.batchContextMap.get( msg.batch() );
                        // only if it is not a duplicate vote
                        batchContext.missingVotes.remove( msg.vms() );
                        if(batchContext.missingVotes.isEmpty()){
                            LOGGER.log(INFO,"Leader: Received all missing votes of batch: "+ msg.batch());
                            // making this implement order-independent, so not assuming batch commit are received in order,
                            // although they are necessarily applied in order both here and in the VMSs
                            // is the current? this approach may miss a batch... so when the batchOffsetPendingCommit finishes,
                            // it must check the batch context match to see whether it is completed
                            if(batchContext.batchOffset == this.batchOffsetPendingCommit){
                                // ack driver earlier
                                this.batchSignalQueue.put(batchContext.lastTid);
                                this.sendCommitRequestToVMSs(batchContext);
                                this.batchOffsetPendingCommit = batchContext.batchOffset + 1;
                            } else {
                                // this probably means some batch complete message got lost
                                LOGGER.log(WARNING,"Leader: Batch "+ msg.batch() +" is not the pending one. Still has to wait for the pending batch ("+this.batchOffsetPendingCommit+") to finish before progressing...");
                            }
                        }
                    }
                    case BatchCommitAck.Payload msg ->
                        // let's just ignore the ACKs. since the terminals have responded, that means the VMSs before them have processed the transactions in the batch
                        // not sure if this is correct since we have to wait for all VMSs to respond...
                        // only when all vms respond with BATCH_COMMIT_ACK we move this ...
                        //        this.batchOffsetPendingCommit = batchContext.batchOffset;
                        LOGGER.log(INFO,"Leader: Batch "+ msg.batch() +" commit ACK received from "+msg.vms());
                    default -> LOGGER.log(WARNING,"Leader: Received an unidentified message type: "+message.getClass().getName());
                }
            } catch (Exception e) {
                LOGGER.log(ERROR,"Leader: Exception caught while looping through coordinatorQueue: "+e);
            }
        }
    }

    /**
     * Only send to non-terminals
     */
    private void sendCommitRequestToVMSs(BatchContext batchContext){
        for(VmsNode vms : this.vmsMetadataMap.values()){
            if(batchContext.terminalVMSs.contains(vms.getIdentifier())) {
                LOGGER.log(INFO,"Leader: Batch commit command not sent to "+ vms.getIdentifier() + " (terminal)");
                continue;
            }

            // has this VMS participated in this batch?
            if(!batchContext.numberOfTasksPerVms.containsKey(vms.getIdentifier())){
                //noinspection StringTemplateMigration
                LOGGER.log(DEBUG,"Leader: Batch commit command will not be sent to "+ vms.getIdentifier() + " because this VMS has not participated in this batch.");
                continue;
            }
            this.vmsWorkerContainer.get(vms.identifier).queue(new BatchCommitCommand.Payload(
                    batchContext.batchOffset,
                    batchContext.previousBatchPerVms.get(vms.getIdentifier()),
                    batchContext.numberOfTasksPerVms.get(vms.getIdentifier())
            ));
        }
    }

    public long getTid() {
        return this.tid;
    }

    public Map<String, VmsNode> getConnectedVMSs() {
        return this.vmsMetadataMap;
    }

    public long getCurrentBatchOffset() {
        return this.currentBatchOffset;
    }

    public long getBatchOffsetPendingCommit() {
        return this.batchOffsetPendingCommit;
    }

    public Map<Integer, IdentifiableNode> getStarterVMSs(){
        return this.starterVMSs;
    }

    public BlockingQueue<Long> getBatchSignalQueue() {
        return this.batchSignalQueue;
    }

}
