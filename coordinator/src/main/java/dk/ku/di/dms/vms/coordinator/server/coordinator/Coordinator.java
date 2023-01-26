package dk.ku.di.dms.vms.coordinator.server.coordinator;

import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.follower.BatchReplication;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.ConsumerVms;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static dk.ku.di.dms.vms.coordinator.server.coordinator.VmsWorker.Command.*;
import static dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.PRESENTATION;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.SERVER_TYPE;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.VMS_TYPE;
import static dk.ku.di.dms.vms.web_common.meta.Issue.Category.UNREACHABLE_NODE;
import static dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata.NodeType.SERVER;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * Class that encapsulates all logic related to issuing of
 * batch commits, transaction aborts, ...
 */
public final class Coordinator extends SignalingStoppableRunnable {

    private final CoordinatorOptions options;

    // this server socket
    private final AsynchronousServerSocketChannel serverSocket;

    // group for channels
    private final AsynchronousChannelGroup group;

    // general tasks, like sending info to VMSs and other servers
    private final ExecutorService taskExecutor;

    // even though we can start with a known number of servers, their payload may have changed after a crash
    private final Map<Integer, ServerIdentifier> servers;

    // for server nodes
    private final Map<Integer, LockConnectionMetadata> serverConnectionMetadataMap;

    /** VMS data structures **/

    // received from program start
    private final Map<Integer, ConsumerVms> starterVMSs;

    /**
     * those received from program start + those that joined later
     * shared with vms workers
     */
    private final Map<String, VmsIdentifier> vmsMetadata;

    private final Map<String, TransactionDAG> transactionMap;

    // if each thread control their own metadata connection, let them manage it
    // private final Map<Integer, ConnectionMetadata> vmsConnectionMetadataMap;

    // the identification of this server
    private final ServerIdentifier me;

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
    private final BlockingQueue<TransactionInput> parsedTransactionRequests;

    /** serialization and deserialization of complex objects **/
    private final IVmsSerdesProxy serdesProxy;

    /**
     * vms workers append to this queue
      */
    record Message(
            Type type,
            Object object
    ) {

        public BatchComplete.Payload asBatchComplete(){
            return (BatchComplete.Payload)object;
        }

        public TransactionAbort.Payload asTransactionAbort(){
            return (TransactionAbort.Payload)object;
        }

        public BatchCommitAck.Payload asBatchCommitAck(){
            return (BatchCommitAck.Payload)object;
        }

        public VmsIdentifier asVmsIdentifier(){
            return (VmsIdentifier)object;
        }

    }

    enum Type {
        BATCH_COMPLETE,
        TRANSACTION_ABORT,
        BATCH_COMMIT_ACK,
        VMS_IDENTIFIER
    }

    private final BlockingQueue<Message> coordinatorQueue;

    public Coordinator(// obtained from leader election or passed by parameter on setup
                       Map<Integer, ServerIdentifier> servers,
                       Map<Integer, LockConnectionMetadata> serverConnectionMetadataMap,
                       // passed by parameter
                       Map<Integer, ConsumerVms> startersVMSs,
                       Map<String, TransactionDAG> transactionMap,
                       // coordinator config
                       ServerIdentifier me,
                       CoordinatorOptions options,
                       // starting batch offset (may come from storage after a crash)
                       long startingBatchOffset,
                       // starting tid (may come from storage after a crash)
                       long startingTid,
                       // queue containing the input transactions. ingestion performed by http server
                       BlockingQueue<TransactionInput> parsedTransactionRequests,
                       IVmsSerdesProxy serdesProxy) throws IOException {
        super();

        // task manager + (eventually) InformLeadershipTask | resendTransactionalInputEvents | general exceptions coming from completable futures
        this.taskExecutor = Executors.newFixedThreadPool(options.getTaskThreadPoolSize());

        // network and executor
        this.group = AsynchronousChannelGroup.withThreadPool(Executors.newWorkStealingPool(options.getGroupThreadPoolSize()));
        this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        this.serverSocket.bind(me.asInetSocketAddress());

        // should come filled from election process
        this.servers = servers == null ? new ConcurrentHashMap<>() : servers;
        this.starterVMSs = startersVMSs;
        this.vmsMetadata = new HashMap<>(10);

        // connection to VMSs
        // this.connectToVmsProtocolMap = new HashMap<>(10);

        // might come filled from election process
        this.serverConnectionMetadataMap = serverConnectionMetadataMap == null ? new HashMap<>() : serverConnectionMetadataMap;
        //this.vmsConnectionMetadataMap = new ConcurrentHashMap<>();
        this.me = me;

        // infra
        this.serdesProxy = serdesProxy;

        // coordinator options
        this.options = options;

        // shared data structure with http handler
        this.parsedTransactionRequests = parsedTransactionRequests;

        // in production, it requires receiving new transaction definitions
        this.transactionMap = Objects.requireNonNull(transactionMap);

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
        previousBatch.seal(startingTid - 1, null);
        this.batchContextMap.put(this.currentBatchOffset - 1, previousBatch);
    }

    /**
     *  This method contains the event loop that contains the main functions of a leader/coordinator
     *  What happens if two nodes declare themselves as leaders? We need some way to let it know
     *  OK (b) Batch management
     * designing leader mode first
     * design follower mode in another class to avoid convoluted code
     * -
     * Going for a different socket to allow for heterogeneous ways for a client to connect with the servers e.g., http.
     * It is also good to better separate resources, so VMSs and followers do not share resources with external clients
     */
    @Override
    public void run() {

        // setup asynchronous listener for new connections
        this.serverSocket.accept( null, new AcceptCompletionHandler());

        // connect to all virtual microservices
        this.setupStarterVMSs();

        while(isRunning()){

            try {

                TimeUnit.of(ChronoUnit.MILLIS).sleep(options.getBatchWindow());

                this.processEventsSentByVmsWorkers();

                this.processTransactionInputEvents();

                this.advanceCurrentBatchAndSpawnSendBatchOfEvents();

            } catch (InterruptedException e) {
                this.logger.warning("Exception captured: "+e.getMessage());
            }

        }

        failSafeClose();

    }

    /**
     * After a leader election, it makes more sense that
     * the leader connects to all known virtual microservices.
     */
    private void setupStarterVMSs() {
        for(ConsumerVms vms : this.starterVMSs.values()){
            vms.vmsWorker = VmsWorker.buildAsStarter(this.me, vms, this.coordinatorQueue, this.group, this.serdesProxy);
            // a cached thread pool would be ok in this case
            new Thread( vms.vmsWorker ).start();
        }
    }

    /**
     * Match output of a vms with the input of another
     * for each vms input event (not generated by the coordinator),
     * find the vms that generated the output
     */
    private List<NetworkNode> findConsumerVMSs(String outputEvent){
        List<NetworkNode> list = new ArrayList<>(2);
        // can be the leader or a vms
        for( VmsIdentifier vms : this.vmsMetadata.values() ){
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

    /**
     * This is where I define whether the connection must be kept alive
     * Depending on the nature of the request:
     * <a href="https://www.baeldung.com/java-nio2-async-socket-channel">...</a>
     * The first read must be a presentation message, informing what is this server (follower or VMS)
     */
    private class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {

            ByteBuffer buffer = null;

            try {

                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                // right now I cannot discern whether it is a VMS or follower. perhaps I can keep alive channels from leader election?

                buffer = MemoryManager.getTemporaryDirectBuffer();

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                // this will be handled by another thread in the group
                channel.read(buffer, buffer, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, ByteBuffer buffer) {
                        processReadAfterAcceptConnection(channel, buffer);
                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer buffer) {
                        MemoryManager.releaseTemporaryDirectBuffer(buffer);
                    }
                });


            } catch (Exception e) {
                logger.warning("Error on accepting connection on " + me);
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
                    channel.write(buffer).get(); // write and forget
                } catch (IOException | ExecutionException | InterruptedException ignored) {
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
            logger.warning("A node is trying to connect without a presentation message:");
            try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch(Exception ignored){}
            return;
        }

        // now let's do the work
        buffer.position(1);

        byte type = buffer.get();
        if(type == SERVER_TYPE){
            processServerPresentationMessage(channel, buffer);
        } else if(type == VMS_TYPE){
            try {
                ConsumerVms vms = new ConsumerVms( channel.getRemoteAddress() );
                vms.vmsWorker = VmsWorker.build( me, vms, coordinatorQueue,
                        channel, group, buffer, serdesProxy);
                // a cached thread pool would be ok in this case
                new Thread( vms.vmsWorker ).start();
            } catch (IOException ignored1) {
                try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch(Exception ignored2){}
            }
        } else {
            // simply unknown... probably a bug?
            logger.warning("Unknown type of client connection. Probably a bug? ");
            try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch(Exception ignored){}
        }

    }

    private void processServerPresentationMessage(AsynchronousSocketChannel channel, ByteBuffer buffer) {
        // server
        // ....
        ServerIdentifier newServer = Presentation.readServer(buffer);

        // check whether this server is known... maybe it has crashed... then we only need to update the respective channel
        if(this.servers.get(newServer.hashCode()) != null){
            LockConnectionMetadata connectionMetadata = this.serverConnectionMetadataMap.get( newServer.hashCode() );
            // update metadata of this node
            this.servers.put( newServer.hashCode(), newServer );
            connectionMetadata.channel = channel;
        } else { // no need for locking here
            this.servers.put( newServer.hashCode(), newServer );
            LockConnectionMetadata connectionMetadata = new LockConnectionMetadata(
                    newServer.hashCode(), SERVER,
                    buffer,
                    MemoryManager.getTemporaryDirectBuffer(1024),
                    channel,
                    new Semaphore(1) );
            this.serverConnectionMetadataMap.put( newServer.hashCode(), connectionMetadata );
            // create a read handler for this connection
            // attach buffer, so it can be read upon completion
            // FIXME why server type creates vms read completion handler?
            // channel.read(buffer, connectionMetadata, new VmsReadCompletionHandler());
        }
    }

    /**
     * Given a set of VMSs involved in the last batch
     * (for easiness can send to all of them for now)
     * send a batch request.
     * -
     * Callback to start batch commit process
     * -
     * we need a cut. all vms must be aligned in terms of tid
     * because the other thread might still be sending intersecting TIDs
     * e.g., vms 1 receives batch 1 tid 1 vms 2 received batch 2 tid 1
     * this is solved by fixing the batch per transaction (and all its events)
     * this is an anomaly across batches
     * -
     * but another problem is that an interleaving can allow the following problem
     * commit handler issues batch 1 to vms 1 with last tid 1
     * but tx-mgr issues event with batch 1 vms 1 last tid 2
     * this can happen because the tx-mgr is still in the loop
     * this is an anomaly within a batch
     * -
     * so we need synchronization to disallow the second
     * -
     * obtain a consistent snapshot of last TIDs for all VMSs
     * the transaction manager must obtain the next batch inside the synchronized block
     * -
     * TODO store the transactions in disk before sending
     */
    private void advanceCurrentBatchAndSpawnSendBatchOfEvents(){

        logger.info("Batch commit run started.");

        // why do I need to replicate vmsTidMap? to restart from this point if the leader fails
        long generateBatch = this.currentBatchOffset;
        BatchContext currBatchContext = this.batchContextMap.get( generateBatch );

        // have we processed any input event since the start of this coordinator thread?
        BatchContext previousBatch = this.batchContextMap.get( generateBatch - 1 );
        if(previousBatch.lastTid == this.tid - 1){
            logger.info("No new transaction since last batch generation. Current batch "+generateBatch+" won't be spawned this time.");
            return;
        }

        // a map of the last tid for each vms
        Map<String,Long> lastTidOfBatchPerVms = this.vmsMetadata.values().stream().collect(
                Collectors.toMap( VmsIdentifier::getIdentifier, VmsIdentifier::getLastTidOfBatch) );

        currBatchContext.seal(this.tid, lastTidOfBatchPerVms);

        // increment batch offset
        this.currentBatchOffset = this.currentBatchOffset + 1;

        // define new batch context
        BatchContext newBatchContext = new BatchContext(this.currentBatchOffset);
        this.batchContextMap.put( this.currentBatchOffset, newBatchContext );

        logger.info("Current batch offset is "+generateBatch+" and new batch offset is "+this.currentBatchOffset);

        // new TIDs will be emitted with the new batch in the transaction manager

        for(VmsIdentifier vms : this.vmsMetadata.values()){

            // update
            vms.previousBatch = vms.batch;
            vms.batch = generateBatch;

            var list = vms.transactionEventsPerBatch(generateBatch);
            if( (list != null && !list.isEmpty()) || currBatchContext.terminalVMSs.contains(vms.vmsIdentifier) ) {
                ((VmsWorker)vms.consumerVms.vmsWorker).workerQueue.add(
                       new VmsWorker.VmsWorkerMessage( SEND_BATCH_OF_EVENTS, new VmsWorker.BatchEventsCommand(
                               generateBatch, // or vms current batch...
                               lastTidOfBatchPerVms.get(vms.getIdentifier()),
                               vms.previousBatch)
                       )
                );
            }
        }

        // the batch commit only has progress (a safety property) the way it is implemented now when future events
        // touch the same VMSs that have been touched by transactions in the last batch.
        // how can we guarantee progress?
        replicateBatchWithReplicas(generateBatch, lastTidOfBatchPerVms);

    }

    private void replicateBatchWithReplicas(long generateBatch, Map<String, Long> lastTidOfBatchPerVms) {
        if ( options.getBatchReplicationStrategy() == NONE) return;

        // to refrain the number of servers increasing concurrently, instead of
        // synchronizing the operation, I can simply obtain the collection first
        // but what happens if one of the servers in the list fails?
        Collection<ServerIdentifier> activeServers = servers.values();
        int nServers = activeServers.size();

        CompletableFuture<?>[] promises = new CompletableFuture[nServers];

        Set<Integer> serverVotes = Collections.synchronizedSet(new HashSet<>(nServers));

        String lastTidOfBatchPerVmsJson = this.serdesProxy.serializeMap(lastTidOfBatchPerVms);

        int i = 0;
        for (ServerIdentifier server : activeServers) {

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
                    channel.setOption(TCP_NODELAY, true);
                    channel.setOption(SO_KEEPALIVE, false);
                    channel.connect(address).get();

                    ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(1024);
                    BatchReplication.write(buffer, generateBatch, lastTidOfBatchPerVmsJson);
                    channel.write(buffer).get();

                    buffer.clear();

                    // immediate read in the same channel
                    channel.read(buffer).get();

                    BatchReplication.BatchReplicationPayload response = BatchReplication.read(buffer);

                    buffer.clear();

                    MemoryManager.releaseTemporaryDirectBuffer(buffer);

                    // assuming the follower always accept
                    if (generateBatch == response.batch()) serverVotes.add(server.hashCode());

                    return null;

                } catch (InterruptedException | ExecutionException | IOException e) {
                    // cannot connect to host
                    logger.warning("Error connecting to host. I am " + me.host + ":" + me.port + " and the target is " + server.host + ":" + server.port);
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

            }, taskExecutor).exceptionallyAsync((x) -> {
                defaultLogError(UNREACHABLE_NODE, server.hashCode());
                return null;
            }, taskExecutor);
            i++;
        }

        // if none, do nothing
        if ( options.getBatchReplicationStrategy() == AT_LEAST_ONE){
            // asynchronous
            // at least one is always necessary
            int j = 0;
            while (j < nServers && serverVotes.size() < 1){
                promises[i].join();
                j++;
            }
            if(serverVotes.isEmpty()){
                logger.warning("The system has entered in a state that data may be lost since there are no followers to replicate the current batch offset.");
            }
        } else if ( options.getBatchReplicationStrategy() == MAJORITY ){

            int simpleMajority = ((nServers + 1) / 2);
            // idea is to iterate through servers, "joining" them until we have enough
            int j = 0;
            while (j < nServers && serverVotes.size() <= simpleMajority){
                promises[i].join();
                j++;
            }

            if(serverVotes.size() < simpleMajority){
                logger.warning("The system has entered in a state that data may be lost since a majority have not been obtained to replicate the current batch offset.");
            }
        } else if ( options.getBatchReplicationStrategy() == ALL ) {
            CompletableFuture.allOf( promises ).join();
            if ( serverVotes.size() < nServers ) {
                logger.warning("The system has entered in a state that data may be lost since there are missing votes to replicate the current batch offset.");
            }
        }

        // for now, we don't have a fallback strategy...

    }

    private final Collection<TransactionInput> transactionRequests = new ArrayList<>(100);

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

        int size = this.parsedTransactionRequests.size();
        if( size == 0 ){
            return;
        }

        this.parsedTransactionRequests.drainTo( this.transactionRequests );

        for(TransactionInput transactionInput : this.transactionRequests){

            TransactionDAG transactionDAG = this.transactionMap.get( transactionInput.name );

            boolean allKnown = true;

            // first make sure the metadata of this vms is known
            // that ensures that, even if the vms is down, eventually it will come to life
            for (TransactionInput.Event inputEvent : transactionInput.events) {
                EventIdentifier event = transactionDAG.inputEvents.get(inputEvent.name);
                VmsIdentifier vms = this.vmsMetadata.get(event.targetVms);
                if(vms == null) {
                    logger.warning("VMS "+event.targetVms+" is unknown to the coordinator");
                    allKnown = false;
                    break;
                }
            }

            if(!allKnown) continue; // got to the next transaction

            // this is the only thread updating this value, so it is by design an atomic operation
            // ++ after variable returns the value before incrementing
            long tid_ = this.tid++;

            // for each input event, send the event to the proper vms
            // assuming the input is correct, i.e., all events are present
            for (TransactionInput.Event inputEvent : transactionInput.events) {

                // look for the event in the topology
                EventIdentifier event = transactionDAG.inputEvents.get(inputEvent.name);

                // get the vms
                VmsIdentifier vms = this.vmsMetadata.get(event.targetVms);

                // write. think about failures/atomicity later
                TransactionEvent.Payload txEvent = TransactionEvent.of(tid_, vms.lastTidOfBatch, this.currentBatchOffset, inputEvent.name, inputEvent.payload);

                // assign this event, so... what? try to send later? if a vms fail, the last event is useless, we need to send the whole batch generated so far...
                var queue = vms.consumerVms.transactionEventsPerBatch.computeIfAbsent(this.currentBatchOffset, k -> new LinkedBlockingDeque<>());
                queue.add(txEvent);

                // a vms, although receiving an event from a "next" batch, cannot yet commit, since
                // there may have additional events to arrive from the current batch
                // so the batch request must contain the last tid of the given vms

                // update for next transaction. this is basically to ensure VMS do not wait for a tid that will never come. TIDs processed by a vms may not be sequential
                vms.lastTidOfBatch = tid_;

            }

            // also update the last tid of the terminals
            for(String vms : transactionDAG.terminals){
                VmsIdentifier vmsId = this.vmsMetadata.get(vms);
                vmsId.lastTidOfBatch = tid_;
            }

            // also update the last tid of the internal VMSs
            // to make sure they wait in case a later transaction (e.g., tid + 1) arrives
            for(String vms : transactionDAG.internalNodes){
                VmsIdentifier vmsId = this.vmsMetadata.get(vms);
                vmsId.lastTidOfBatch = tid_;
            }

            // add terminal to the set... so cannot be immutable when the batch context is created...
            this.batchContextMap.get(this.currentBatchOffset).terminalVMSs.addAll( transactionDAG.terminals );

        }

    }

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

        // it is ok to keep this loop. at some point events from VMSs will stop arriving
        while(!this.coordinatorQueue.isEmpty()){

            try {
                Message message = this.coordinatorQueue.take();

                switch(message.type){

                    case VMS_IDENTIFIER -> {
                        // update metadata of this node so coordinator can reason about data dependencies
                         this.vmsMetadata.put( message.asVmsIdentifier().getIdentifier(), message.asVmsIdentifier() );

                        // if all metadata, from all starter vms have arrived, then send the signal to them
                        if(this.vmsMetadata.size() == this.starterVMSs.size()){

                            Map<String, List<NetworkNode>> vmsConsumerSet = new HashMap<>();

                            for(VmsIdentifier vmsIdentifier : this.vmsMetadata.values()) {

                                ConsumerVms vms = this.starterVMSs.get( vmsIdentifier.hashCode() );
                                if(vms == null) continue;

                                // build consumer set dynamically
                                // for each output event, find the consumer VMSs
                                for (VmsEventSchema eventSchema : vmsIdentifier.outputEventSchema.values()) {
                                    List<NetworkNode> nodes = this.findConsumerVMSs(eventSchema.eventName);
                                    if (!nodes.isEmpty())
                                        vmsConsumerSet.put(eventSchema.eventName, nodes);
                                }

                                String mapStr = "";
                                if (!vmsConsumerSet.isEmpty()) {
                                    mapStr = this.serdesProxy.serializeConsumerSet(vmsConsumerSet);
                                }

                                ((VmsWorker) vms.vmsWorker).workerQueue.add( new VmsWorker.VmsWorkerMessage( SEND_CONSUMER_SET, mapStr ));

                            }

                        }

                    }

                    case TRANSACTION_ABORT -> {
                        // send abort to all VMSs...
                        // later we can optimize the number of messages since some VMSs may not need to receive this abort

                        // cannot commit the batch unless the VMS is sure there will be no aborts...
                        // this is guaranteed by design, since the batch complete won't arrive unless all events of the batch arrive at the terminal VMSs

                        TransactionAbort.Payload msg = message.asTransactionAbort();

                        this.batchContextMap.get( msg.batch() ).tidAborted = msg.tid();

                        // can reuse the same buffer since the message does not change across VMSs like the commit request
                        for(VmsIdentifier vms : this.vmsMetadata.values()){
                            // don't need to send to the vms that aborted
                            if(vms.getIdentifier().equalsIgnoreCase( msg.vms() )) continue;
                            ((VmsWorker)vms.consumerVms.vmsWorker).workerQueue.add( new VmsWorker.VmsWorkerMessage( SEND_TRANSACTION_ABORT, msg.tid()));
                        }
                    }

                    case BATCH_COMPLETE -> {
                        // what if ACKs from VMSs take too long? or never arrive?
                        // need to deal with intersecting batches? actually just continue emitting for higher throughput
                        BatchComplete.Payload msg = message.asBatchComplete();
                        BatchContext batchContext = this.batchContextMap.get( msg.batch() );
                        // only if it is not a duplicate vote
                        if( batchContext.missingVotes.remove( msg.vms() ) ){

                            // making this implement order-independent, so not assuming batch commit are received in order,
                            // although they are necessarily applied in order both here and in the VMSs
                            // is the current? this approach may miss a batch... so when the batchOffsetPendingCommit finishes,
                            // it must check the batch context match to see whether it is completed
                            if( batchContext.batchOffset == this.batchOffsetPendingCommit && batchContext.missingVotes.size() == 0 ){
                                this.sendCommitRequestToVMSs(batchContext);
                                this.batchOffsetPendingCommit = batchContext.batchOffset + 1;
                            }

                        }
                    }

                    case BATCH_COMMIT_ACK -> {
                        // let's just ignore the ACKs. since the terminals have responded, that means the VMSs before them have processed the transactions in the batch
                        // not sure if this is correct since we have to wait for all VMSs to respond...
                        // only when all vms respond with BATCH_COMMIT_ACK we move this ...
                        //        this.batchOffsetPendingCommit = batchContext.batchOffset;
                        BatchCommitAck.Payload msg = message.asBatchCommitAck();
                        this.logger.info("Batch "+ msg.batch() +" commit ACK received from "+msg.vms());
                    }

                }

            } catch (InterruptedException e) {
                this.logger.warning("Exception caught while looping through coordinatorQueue: "+e.getMessage());
            }


        }

    }

    private void sendCommitRequestToVMSs(BatchContext batchContext){
        for(VmsIdentifier vms : this.vmsMetadata.values()){
            ((VmsWorker)vms.consumerVms.vmsWorker).workerQueue.add( new VmsWorker.VmsWorkerMessage(SEND_BATCH_COMMIT, new BatchCommitRequest.Payload(
                    batchContext.batchOffset,
                    batchContext.lastTidOfBatchPerVms.get(vms.getIdentifier()))
            ));
        }
    }

}
