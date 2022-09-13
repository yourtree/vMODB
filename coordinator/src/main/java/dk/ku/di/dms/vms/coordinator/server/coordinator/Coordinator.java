package dk.ku.di.dms.vms.coordinator.server.coordinator;

import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.transaction.BatchContext;
import dk.ku.di.dms.vms.coordinator.server.coordinator.transaction.TransactionManagerContext;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.follower.BatchReplication;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Heartbeat;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.buffer.BufferManager;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.Issue;
import dk.ku.di.dms.vms.web_common.network.NetworkRunnable;
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.SERVER;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.VMS;
import static dk.ku.di.dms.vms.web_common.meta.Issue.Category.*;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * Class that encapsulates all logic related to issuing of
 * batch commits, transaction aborts, ...
 */
public final class Coordinator extends NetworkRunnable {

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
    private final Map<Integer, ConnectionMetadata> serverConnectionMetadataMap;

    /** VMS data structures **/
    private final Map<Integer, VmsIdentifier> VMSs;

    private final Map<Integer, List<NetworkNode>> vmsConsumerSet;

    private final Map<String, TransactionDAG> transactionMap;

    private final Map<Integer, ConnectionMetadata> vmsConnectionMetadataMap;

    // the identification of this server
    private final ServerIdentifier me;

    // must update the "me" on snapshotting (i.e., committing)
    private long tid;

    // initial design, maybe readwrite lock might be better in case several reading threads
    private final Object batchCommitLock;

    // the offset of the pending batch commit
    private long batchOffsetPendingCommit;

    // the current batch on which new transactions are being generated for
    private volatile long batchOffset;

    // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    // defines how the batch metadata is replicated across servers
    private final BatchReplicationStrategy batchReplicationStrategy;

    // transaction requests coming from the http event loop
    private final BlockingQueue<TransactionInput> parsedTransactionRequests;

    /** serialization and deserialization of complex objects **/
    private final IVmsSerdesProxy serdesProxy;

    private final TransactionManagerContext txManagerCtx;

    public Coordinator(AsynchronousServerSocketChannel serverSocket,
                       AsynchronousChannelGroup group,
                       ExecutorService taskExecutor,
                       Map<Integer, ServerIdentifier> servers,
                       Map<Integer, VmsIdentifier> VMSs,
                       Map<String, TransactionDAG> transactionMap,
                       Map<Integer, List<NetworkNode>> vmsDependenceSet, // built from the transactionMap
                       Map<Integer, ConnectionMetadata> serverConnectionMetadataMap,
                       ServerIdentifier me,
                       CoordinatorOptions options,
                       long startingTid,
                       long batchOffset,
                       BatchReplicationStrategy batchReplicationStrategy,
                       BlockingQueue<TransactionInput> parsedTransactionRequests,
                       IVmsSerdesProxy serdesProxy
    ) {
        super();

        // network and executor
        this.serverSocket = Objects.requireNonNull(serverSocket);
        this.group = group; // can be null

        // task manager + (eventually) InformLeadershipTask | resendTransactionalInputEvents | general exceptions coming from completable futures
        this.taskExecutor = taskExecutor != null ? taskExecutor : Executors.newFixedThreadPool(2);

        // should come filled from election process
        this.servers = servers == null ? new ConcurrentHashMap<>() : servers;
        this.VMSs = VMSs == null ? new ConcurrentHashMap<>() : VMSs;
        this.batchCommitLock = new Object();

        // might come filled from election process
        this.serverConnectionMetadataMap = serverConnectionMetadataMap == null ? new HashMap<>() : serverConnectionMetadataMap;
        this.vmsConnectionMetadataMap = new HashMap<>();
        this.me = me;

        // infra
        this.serdesProxy = serdesProxy;

        // coordinator options
        this.options = options;

        // transactions
        this.tid = startingTid;
        this.parsedTransactionRequests = parsedTransactionRequests; // shared data structure
        this.transactionMap = Objects.requireNonNull(transactionMap); // in production, it requires receiving new transaction definitions
        this.vmsConsumerSet = vmsDependenceSet;
        this.txManagerCtx = new TransactionManagerContext(
                new LinkedBlockingQueue<Byte>(),
                new ConcurrentLinkedQueue<>(),
                new ConcurrentLinkedQueue<>() );

        // batch commit
        this.batchOffset = batchOffset;
        this.batchOffsetPendingCommit = batchOffset;
        this.batchContextMap = new ConcurrentHashMap<>();
        this.batchReplicationStrategy = batchReplicationStrategy;
    }

    /**
     * This method contains the main loop that contains the main functions of a leader
     *  What happens if two nodes declare themselves as leaders? We need some way to let it know
     *  OK (b) Batch management
     * designing leader mode first
     * design follower mode in another class to avoid convoluted code
     *
     * Going for a different socket to allow for heterogeneous ways for a client to connect with the servers e.g., http.
     * It is also good to better separate resources, so VMSs and followers do not share resources with external clients
     */
    @Override
    public void run() {

        // connect to all virtual microservices
        connectToVMSs();

        // setup asynchronous listener for new connections
        serverSocket.accept( null, new AcceptCompletionHandler());

        // only submit when there are events to react to
        // perhaps not a good idea to have this thread in a pool, since this thread will get blocked
        // BUT, it is simply about increasing the pool size with +1...
        TransactionManager txManager = new TransactionManager();
        Future<?> txManagerTask = taskExecutor.submit( txManager );
        // txManager.run();

        // they can run concurrently, max 3 at a time always
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool( 3 );

        // callbacks
        ScheduledFuture<?> parseTask = scheduledExecutorService.scheduleWithFixedDelay(this::processTransactionInputEvents, 0L, 10000, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> heartbeatTask = scheduledExecutorService.scheduleAtFixedRate(this::sendHeartbeats, 0L, options.getHeartbeatTimeout() - options.getHeartbeatSlack(), TimeUnit.MILLISECONDS);
        ScheduledFuture<?> batchCommitTask = scheduledExecutorService.scheduleAtFixedRate(this::spawnBatchCommit, 0L, options.getBatchWindow(), TimeUnit.MILLISECONDS);

        // if the transaction manager thread blocks (e.g., waiting for a queue), the thread is not delivered back to the pool

        while(isRunning()){

            try {
                // issueQueue.poll(5, TimeUnit.MINUTES); // blocking, this thread should only act when necessary
                issueQueue.take();
            } catch (InterruptedException ignored) {} // not going to be interrupted by our code

        }

        failSafeClose(batchCommitTask, heartbeatTask, parseTask, txManagerTask, txManager);

    }

    /**
     * After a leader election, it makes more sense that
     * the leader connects to all known virtual microservices.
     */
    private void connectToVMSs(){

        for(VmsIdentifier vms : VMSs.values()){

            try {

                InetSocketAddress address = new InetSocketAddress(vms.host, vms.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                withDefaultSocketOptions(channel);

                channel.connect(address).get();

                ConnectionMetadata connMetadata = new ConnectionMetadata(vms.hashCode(), VMS,
                        BufferManager.loanByteBuffer(), BufferManager.loanByteBuffer(), channel, new Semaphore(1));
                vmsConnectionMetadataMap.put(vms.hashCode(), connMetadata);

                // get dependence set of given VMS
                List<NetworkNode> consumerSet = vmsConsumerSet.get( vms.hashCode() );

                String consumerSetJson = serdesProxy.serializeList(consumerSet);

                // write presentation
                Presentation.writeServer( connMetadata.writeBuffer, this.me, consumerSetJson );

                connMetadata.channel.write( connMetadata.writeBuffer ).get();

                connMetadata.writeBuffer.clear();

                // now read the presentation message of a vms...
//                    channel.read(connMetadata.readBuffer).get();
//
//                    VmsIdentifier vmsIdentifier = Presentation.readVms( connMetadata.readBuffer, serdesProxy );
//
//                    VMSs.put( vmsIdentifier.hashCode(), vmsIdentifier );

                channel.read(connMetadata.readBuffer, connMetadata, new ReadCompletionHandler());


            } catch(ExecutionException | InterruptedException| IOException ignored){ }

        }

    }

    private void failSafeClose(ScheduledFuture<?> batchCommitTask, ScheduledFuture<?> heartbeatTask,
                               ScheduledFuture<?> parseTask, Future<?> txManagerTask, TransactionManager txManager){
        // safe close
        batchCommitTask.cancel(false);
        heartbeatTask.cancel(false); // do not interrupt given the lock management
        parseTask.cancel(false);
        if(!txManagerTask.isDone())
            txManager.stop();
        try { serverSocket.close(); } catch (IOException ignored) {}
    }

    /**
     * Reuses the thread from the socket thread pool, instead of assigning a specific thread
     * Removes thread context switching costs.
     * This thread should not block.
     * The idea is to decode the message and deliver back to main loop as soon as possible
     *
     * This thread must be set free as soon as possible
     */
    private class ReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        // is it an abort, a commit response?
        // it cannot be replication because have opened another channel for that

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            // decode message by getting the first byte
            byte type = connectionMetadata.readBuffer.get();

            // from all terminal VMSs involved in the last batch
            if(type == BATCH_COMPLETE){

                // don't actually need the host and port in the payload since we have the attachment to this read operation...
                BatchComplete.Payload response = BatchComplete.read( connectionMetadata.readBuffer );
                txManagerCtx.batchCompleteEvents().add( response );
                txManagerCtx.actionQueue.add(BATCH_COMPLETE); // must have a context, i.e., what batch, the last?

                // if one abort, no need to keep receiving
                // actually it is unclear in which circumstances a vms would respond no... probably in case it has not received an ack from an aborted commit response?
                // because only the aborted transaction will be rolled back

            } else if (type == TX_ABORT){

                // get information of what
                TransactionAbort.Payload response = TransactionAbort.read(connectionMetadata.readBuffer);
                txManagerCtx.transactionAbortEvents().add( response );
                txManagerCtx.actionQueue.add(TX_ABORT);

            } else {
                logger.warning("Unknown message received.");
            }

            connectionMetadata.readBuffer.clear();

            connectionMetadata.channel.read( connectionMetadata.readBuffer, connectionMetadata, this );
        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            connectionMetadata.readBuffer.clear();
            if(connectionMetadata.channel.isOpen()){
                connectionMetadata.channel.read( connectionMetadata.readBuffer, connectionMetadata, this );
            } else {

                // modify status
                if(connectionMetadata.nodeType == VMS){
                    VMSs.get(connectionMetadata.key).off();
                } else {
                    servers.get(connectionMetadata.key).off();
                }

            }
        }

    }

    /**
     * This is where I define whether the connection must be kept alive
     * Depending on the nature of the request
     * https://www.baeldung.com/java-nio2-async-socket-channel
     * The first read must be a presentation message, informing what is this server (follower or VMS)
     */
    private class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {

            ByteBuffer buffer = null;

            try {

                // do I need this check? I believe that if the operation completed and keep alive connection, this is always true
//                if ((channel != null) && (channel.isOpen())) {}
                withDefaultSocketOptions(channel);

                // right now I cannot discern whether it is a VMS or follower. perhaps I can keep alive channels from leader election?

                buffer = BufferManager.loanByteBuffer();

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                Future<Integer> readFuture = channel.read( buffer );
                readFuture.get();

                if( !acceptConnection(channel, buffer) ) {
                    BufferManager.returnByteBuffer(buffer);
                }

            } catch(Exception e){
                // return buffer to queue
                if(channel != null && !channel.isOpen() && buffer != null){
                    BufferManager.returnByteBuffer(buffer);
                }
            } finally {
                // continue listening
                if (serverSocket.isOpen()){
                    serverSocket.accept(null, this);
                }
            }

        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            }
        }

        /**
         *
         * Process Accept connection request
         *
         * Task for informing the server running for leader that a leader is already established
         * We would no longer need to establish connection in case the {@link dk.ku.di.dms.vms.coordinator.election.ElectionWorker}
         * maintains the connections.
         */
        private boolean acceptConnection(AsynchronousSocketChannel channel, ByteBuffer buffer){

            // message identifier
            byte messageIdentifier = buffer.get(0);

            if(messageIdentifier == VOTE_REQUEST || messageIdentifier == VOTE_RESPONSE){
                // so I am leader, and I respond with a leader request to this new node
                // taskExecutor.submit( new ElectionWorker.WriteTask( LEADER_REQUEST, server ) );
                // would be better to maintain the connection open.....
                buffer.clear();

                if(channel.isOpen()) {
                    LeaderRequest.write(buffer, me);
                    try {
                        channel.write(buffer);
                        channel.close();
                    } catch(IOException ignored) {}
                }

                return false;
            }

            if(messageIdentifier == LEADER_REQUEST){
                // buggy node intending to pose as leader...
                // issueQueue.add(  )
                return false;
            }

            // if it is not a presentation, drop connection
            if(messageIdentifier != PRESENTATION){
                return false;
            }

            // now let's do the work

            buffer.position(1);

            byte type = buffer.get();
            if(type == 0){
                // server
                // ....
                ServerIdentifier newServer = Presentation.readServer(buffer);

                // check whether this server is known... maybe it has crashed... then we only need to update the respective channel
                if(servers.get(newServer.hashCode()) != null){

                    ConnectionMetadata connectionMetadata = serverConnectionMetadataMap.get( newServer.hashCode() );

                    // lock to refrain other threads from using old metadata
                    connectionMetadata.writeLock.acquireUninterruptibly();

                    // update metadata of this node
                    servers.put( newServer.hashCode(), newServer );

                    connectionMetadata.channel = channel;

                    connectionMetadata.writeLock.release();

                } else { // no need for locking here

                    servers.put( newServer.hashCode(), newServer );

                    ConnectionMetadata connectionMetadata = new ConnectionMetadata(
                            newServer.hashCode(), SERVER, buffer, BufferManager.loanByteBuffer(), channel,
                            new Semaphore(1) );
                    serverConnectionMetadataMap.put( newServer.hashCode(), connectionMetadata );
                    // create a read handler for this connection
                    // attach buffer, so it can be read upon completion
                    channel.read(buffer, connectionMetadata, new ReadCompletionHandler());

                }
            } else if(type == 1){ // vms

                VmsIdentifier newVms = Presentation.readVms(buffer, serdesProxy);

                if(VMSs.get( newVms.hashCode() ) != null){

                    // vms reconnecting
                    newVms = VMSs.get( newVms.hashCode() );

                    ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( newVms.hashCode() );

                    // lock to refrain other threads from using old metadata
                    connectionMetadata.writeLock.acquireUninterruptibly();

                    // update metadata of this node
                    VMSs.put( newVms.hashCode(), newVms );

                    // update channel and possibly the address
                    connectionMetadata.channel = channel;
                    connectionMetadata.key = newVms.hashCode();

                    connectionMetadata.writeLock.release();

                    // let's vms is back online from crash or is simply a new vms.
                    // we need to send batch info or simply the vms assume...
                    // if a vms crashed, it has lost all the events since the last batch commit, so need to resend it now

                    List<TransactionEvent.Payload> list = newVms.transactionEventsPerBatch.get( newVms.lastBatch + 1 );

                    if(list != null) {// avoiding creating a thread for nothing
                        taskExecutor.submit(() -> resendTransactionalInputEvents(
                                connectionMetadata, list ));
                    }

                    channel.read(buffer, connectionMetadata, new ReadCompletionHandler());

                } else {
                    VMSs.put( newVms.hashCode(), newVms );
                    ConnectionMetadata connectionMetadata = new ConnectionMetadata(
                            newVms.hashCode(), VMS, buffer, BufferManager.loanByteBuffer(), channel, new Semaphore(1) );
                    vmsConnectionMetadataMap.put( newVms.hashCode(), connectionMetadata );
                    channel.read(buffer, connectionMetadata, new ReadCompletionHandler());
                }

            } else {
                // simply unknown... probably a bug?
                try{
                    if(channel.isOpen()) {
                        channel.close();
                    }
                } catch(Exception ignored){}
                return false;

            }

            return true;
        }

    }

    /**
     * A thread will execute this piece of code to liberate the "Accept" thread handler
     * This only works for input events. In case of internal events, the VMS needs to get that from the precedence VMS.
     */
    private void resendTransactionalInputEvents(ConnectionMetadata connectionMetadata, List<TransactionEvent.Payload> list){

        // assuming everything is lost, we have to resend...
        if( list != null ){
            connectionMetadata.writeLock.acquireUninterruptibly();
            for( TransactionEvent.Payload txEvent : list ){
                TransactionEvent.write( connectionMetadata.writeBuffer, txEvent);
                Future<?> task = connectionMetadata.channel.write( connectionMetadata.writeBuffer );
                try { task.get(); } catch (InterruptedException | ExecutionException ignored) {}
                connectionMetadata.writeBuffer.clear();
            }
        }

    }

    /**
     * Given a set of VMSs involved in the last batch
     * (for easiness can send to all of them for now)
     * send a batch request.
     *
     * Callback to start batch commit process
     *
     * TODO store the transactions in disk before sending
     */
    private void spawnBatchCommit(){

        // we need a cut. all vms must be aligned in terms of tid
        // because the other thread might still be sending intersecting TIDs
        // e.g., vms 1 receives batch 1 tid 1 vms 2 received batch 2 tid 1
        // this is solved by fixing the batch per transaction (and all its events)
        // this is an anomaly across batches

        // but another problem is that an interleaving can allow the following problem
        // commit handler issues batch 1 to vms 1 with last tid 1
        // but tx-mgr issues event with batch 1 vms 1 last tid 2
        // this can happen because the tx-mgr is still in the loop
        // this is an anomaly within a batch

        // so we need synchronization to disallow the second

        // obtain a consistent snapshot of last TIDs for all VMSs
        // the transaction manager must obtain the next batch inside the synchronized block

        // why do I need to replicate vmsTidMap? to restart from this point if the leader fails
        Map<String,Long> lastTidOfBatchPerVms;
        long currBatch;
        synchronized (batchCommitLock) {

            currBatch = batchOffset;

            BatchContext currBatchContext = batchContextMap.get( currBatch );
            currBatchContext.seal();

            // a map of the last tid for each vms
            lastTidOfBatchPerVms = VMSs.values().stream().collect(
                    Collectors.toMap( VmsIdentifier::getIdentifier, VmsIdentifier::getLastTid ) );

            // define new batch context
            // need to get
            BatchContext newBatchContext = new BatchContext(batchOffset, lastTidOfBatchPerVms);
            batchContextMap.put( batchOffset, newBatchContext );

            long newBatch = ++batchOffset; // first increase the value and then execute the statement

            logger.info("Current batch offset is "+currBatch+" and new batch offset is "+newBatch);

        }
        // new TIDs will be emitted with the new batch in the transaction manager

        // to refrain the number of servers increasing concurrently, instead of
        // synchronizing the operation, I can simply obtain the collection first
        // but what happens if one of the servers in the list fails?
        Collection<ServerIdentifier> activeServers = servers.values();
        int nServers = activeServers.size();

        CompletableFuture<?>[] promises = new CompletableFuture[nServers];

        Set<Integer> serverVotes = Collections.synchronizedSet( new HashSet<>(nServers) );

        String lastTidOfBatchPerVmsJson = serdesProxy.serializeMap( lastTidOfBatchPerVms );

        int i = 0;
        for(ServerIdentifier server : activeServers){

            if(!server.isActive()) continue;
            promises[i] = CompletableFuture.supplyAsync( () ->
            {
                // could potentially use another channel for writing commit-related messages...
                // could also just open and close a new connection
                // actually I need this since I must read from this thread instead of relying on the
                // read completion handler
                AsynchronousSocketChannel channel = null;
                try {

                    InetSocketAddress address = new InetSocketAddress(server.host, server.port);
                    channel = AsynchronousSocketChannel.open(group);
                    channel.setOption( TCP_NODELAY, true );
                    channel.setOption( SO_KEEPALIVE, false );
                    channel.connect(address).get();

                    ByteBuffer buffer = BufferManager.loanByteBuffer();
                    BatchReplication.write( buffer, currBatch, lastTidOfBatchPerVmsJson );
                    channel.write( buffer ).get();

                    buffer.clear();

                    // immediate read in the same channel
                    channel.read( buffer ).get();

                    BatchReplication.BatchReplicationPayload response = BatchReplication.read( buffer );

                    buffer.clear();

                    BufferManager.returnByteBuffer(buffer);

                    // assuming the follower always accept
                    if(currBatch == response.batch()) serverVotes.add( server.hashCode() );

                    return null;

                } catch (InterruptedException | ExecutionException | IOException e) {
                    // cannot connect to host
                    logger.warning("Error connecting to host. I am "+ me.host+":"+me.port+" and the target is "+ server.host+":"+ server.port);
                    return null;
                } finally {
                    if(channel != null && channel.isOpen()) {
                        try {
                            channel.close();
                        } catch (IOException ignored) {}
                    }
                }

                // these threads need to block to wait for server response

            }, taskExecutor).exceptionallyAsync( (x) -> {
                defaultLogError( UNREACHABLE_NODE, server.hashCode() );
                return null;
            }, taskExecutor);
            i++;
        }

        if ( batchReplicationStrategy == AT_LEAST_ONE){
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
        } else if ( batchReplicationStrategy == MAJORITY ){

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
        } else if ( batchReplicationStrategy == ALL ) {
            CompletableFuture.allOf( promises ).join();
            if ( serverVotes.size() < nServers ) {
                logger.warning("The system has entered in a state that data may be lost since there are missing votes to replicate the current batch offset.");
            }
        }

        // for now, we don't have a fallback strategy...

    }

    /**
     * (a) Heartbeat sending to avoid followers to initiate a leader election. That can still happen due to network latency.
     *
     * Given a list of known followers, send to each a heartbeat
     * Heartbeats must have precedence over other writes, since they
     * avoid the overhead of starting a new election process in remote nodes
     * and generating new messages over the network.
     *
     * I can implement later a priority-based scheduling of writes.... maybe some Java DT can help?
     */
    private void sendHeartbeats() {
        logger.info("Sending vote requests. I am "+ me.host+":"+me.port);
        for(ServerIdentifier server : servers.values()){
            ConnectionMetadata connectionMetadata = serverConnectionMetadataMap.get( server.hashCode() );
            if(connectionMetadata.channel != null) {
                Heartbeat.write(connectionMetadata.writeBuffer, me);
                connectionMetadata.writeLock.acquireUninterruptibly();
                connectionMetadata.channel.write( connectionMetadata.writeBuffer, connectionMetadata, new WriteCompletionHandler() );
            } else {
                issueQueue.add( new Issue( CHANNEL_NOT_REGISTERED, server.hashCode() ) );
            }
        }
    }

    private static final class WriteCompletionHandler0 implements CompletionHandler<Integer, ConnectionMetadata> {

        private int overflowPos;

        public WriteCompletionHandler0(){
            this.overflowPos = 0;
        }

        public WriteCompletionHandler0(int pos){
            this.overflowPos = pos;
        }

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            if(overflowPos == 0) {
                connectionMetadata.writeBuffer.rewind();
                return;
            }

            int initPos = connectionMetadata.writeBuffer.position() + 1;

            byte[] overflowContent = connectionMetadata.writeBuffer.
                    slice( initPos, overflowPos ).array();

            connectionMetadata.writeBuffer.rewind();

            connectionMetadata.writeBuffer.put( overflowContent );

        }

        /**
         * If failed, probably the VMS is down.
         * When getting back, the resend task will write data to the buffer again
         */
        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.rewind();
        }

    }

    /**
     * Allows to reuse the thread pool assigned to socket to complete the writing
     * That refrains the main thread and the TransactionManager to block, thus allowing its progress
     */
    private static final class WriteCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.rewind();
            connectionMetadata.writeLock.release();
        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.rewind();
            connectionMetadata.writeLock.release();
        }

    }

    /**
     *  Should read in a proportion that matches the batch and heartbeat window, otherwise
     *  how long does it take to process a batch of input transactions?
     *  instead of trying to match the rate of processing, perhaps we can create read tasks
     *
     * TODO do not send the transactions. the batch commit should perform this
     *  only parse then and save in memory data structure
     *
     *  processing the transaction input and creating the
     *  corresponding events
     *  the network buffering will send
     */
    private void processTransactionInputEvents(){

        int size = parsedTransactionRequests.size();
        if( parsedTransactionRequests.size() == 0 ){
            return;
        }

        Collection<TransactionInput> transactionRequests = new ArrayList<>(size + 50); // threshold, for concurrent appends
        parsedTransactionRequests.drainTo( transactionRequests );

        for(TransactionInput transactionInput : transactionRequests){

            // this is the only thread updating this value, so it is by design an atomic operation
            long tid_ = ++tid;

            TransactionDAG transactionDAG = transactionMap.get( transactionInput.name );

            // should find a way to continue emitting new transactions without stopping this thread
            // non-blocking design
            // having the batch here guarantees that all input events of the same tid does
            // not belong to different batches

            // to allow for a snapshot of the last TIDs of each vms involved in this transaction
            long batch_;
            synchronized (batchCommitLock) {
                // get the batch here since the batch handler is also incrementing it inside a synchronized block
                batch_ = batchOffset;
            }

            // for each input event, send the event to the proper vms
            // assuming the input is correct, i.e., all events are present
            for (TransactionInput.Event inputEvent : transactionInput.events) {

                // look for the event in the topology
                EventIdentifier event = transactionDAG.topology.get(inputEvent.name);

                // get the vms
                VmsIdentifier vms = VMSs.get(event.vms.hashCode());

                // get the connection metadata
                ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

                // we could employ deterministic writes to the channel, that is, an order that would not require locking for writes
                // we could possibly open a channel per write operation, but... what are the consequences of too much opened connections?
                // FIXME make this thread handles the resend, this way we don't need locks
                connectionMetadata.writeLock.acquireUninterruptibly();

                // get current pos to later verify whether
                // int currentPos = connectionMetadata.writeBuffer.position();
                connectionMetadata.writeBuffer.mark();

                // write. think about failures/atomicity later
                TransactionEvent.Payload txEvent = TransactionEvent.write(connectionMetadata.writeBuffer, tid_, vms.lastTid, batch_, inputEvent.name, inputEvent.payload);

                // always assuming the buffer capacity is able to sustain the next write...
                if(connectionMetadata.writeBuffer.position() > batchBufferSize){
                    int overflowPos = connectionMetadata.writeBuffer.position();

                    // return to marked position, so the overflow content is not written
                    connectionMetadata.writeBuffer.reset();

                    connectionMetadata.channel.write(connectionMetadata.writeBuffer,
                            connectionMetadata, new WriteCompletionHandler0(overflowPos));

                }

                // assign this event, so... what? try to send later? if a vms fail, the last event is useless, we need to send the whole batch generated so far...
                List<TransactionEvent.Payload> list = vms.transactionEventsPerBatch.get(batch_);
                if (list == null ){
                    vms.transactionEventsPerBatch.put(batch_, new ArrayList<>());
                } else {
                    list.add(txEvent);
                }

                // a vms, although receiving an event from a "next" batch, cannot yet commit, since
                // there may have additional events to arrive from the current batch
                // so the batch request must contain the last tid of the given vms

                // update for next transaction
                vms.lastTid = tid_;

            }

            // add terminal to the set... so cannot be immutable when the batch context is created...
            batchContextMap.get(batch_).terminalVMSs.addAll( transactionDAG.terminals );

        }

    }

    /**
     * This task assumes the channels are already established
     * Cannot have two threads writing to the same channel at the same time
     * A transaction manager is responsible for assigning TIDs to incoming transaction requests
     * This task also involves making sure the writes are performed successfully
     * A writer manager is responsible for defining strategies, policies, safety guarantees on
     * writing concurrently to channels.
     *
     * TODO make this the only thread writing to the socket
     *      that requires buffers to read what should be written (for the spawn batch?)
     *      network flutuaction is another problem...
     *      - resend bring here
     *      - process transaction input events here
     *
     */
    public class TransactionManager extends StoppableRunnable {

        @Override
        public void run() {

            while(isRunning()){

                try {
                    // https://web.mit.edu/6.005/www/fa14/classes/20-queues-locks/message-passing/
                    byte action = txManagerCtx.actionQueue().take();

                    // do we have any transaction-related event?
                    switch(action){
                        case BATCH_COMPLETE -> {

                            // what if ACKs from VMSs take too long? or never arrive?
                            // need to deal with intersecting batches? actually just continue emitting for higher throughput

                            BatchComplete.Payload msg = txManagerCtx.batchCompleteEvents.remove();

                            BatchContext batchContext = batchContextMap.get( msg.batch() );

                            // only if it is not a duplicate vote
                            if( batchContext.missingVotes.remove( msg.vms() ) ){

                                // making this implement order-independent, so not assuming batch commit are received in order, although they are necessarily applied in order both here and in the VMSs
                                // is the current? this approach may miss a batch... so when the batchOffsetPendingCommit finishes, it must check the batch context match to see whether it is completed
                                if( batchContext.batchOffset == batchOffsetPendingCommit && batchContext.missingVotes.size() == 0 ){

                                    sendCommitRequestToVMSs(batchContext);

                                    // is the next batch completed already?
                                    batchContext = batchContextMap.get( ++batchOffsetPendingCommit );
                                    while(batchContext.missingVotes.size() == 0){
                                        sendCommitRequestToVMSs(batchContext);
                                        batchContext = batchContextMap.get( ++batchOffsetPendingCommit );
                                    }

                                }

                            }


                        }
                        case TX_ABORT -> {

                            // send abort to all VMSs...
                            // later we can optimize the number of messages since some VMSs may not need to receive this abort

                            // cannot commit the batch unless the VMS is sure there will be no aborts...
                            // this is guaranteed by design, since the batch complete won't arrive unless all events of the batch arrive at the terminal VMSs

                            TransactionAbort.Payload msg = txManagerCtx.transactionAbortEvents.remove();

                            // can reuse the same buffer since the message does not change across VMSs like the commit request
                            for(VmsIdentifier vms : VMSs.values()){

                                // don't need to send to the vms that aborted
                                if(vms.getIdentifier().equalsIgnoreCase( msg.vms() )) continue;

                                ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

                                ByteBuffer buffer = BufferManager.loanByteBuffer();

                                TransactionAbort.write(buffer, msg);

                                // must lock first before writing to write buffer
                                connectionMetadata.writeLock.acquireUninterruptibly();

                                try { connectionMetadata.channel.write(buffer).get(); } catch (ExecutionException ignored) {}

                                connectionMetadata.writeLock.release();

                                buffer.clear();

                                BufferManager.returnByteBuffer(buffer);

                            }


                        }
                    }
                } catch (InterruptedException e) {
                    issueQueue.add( new Issue( TRANSACTION_MANAGER_STOPPED, me.hashCode() ) );
                }

            }

        }

        // this could be asynchronously
        private void sendCommitRequestToVMSs(BatchContext batchContext){

            for(VmsIdentifier vms : VMSs.values()){

                ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

                // must lock first before writing to write buffer
                connectionMetadata.writeLock.acquireUninterruptibly();

                // having more than one write buffer does not help us, since the connection is the limitation (one writer at a time)
                BatchCommitRequest.write( connectionMetadata.writeBuffer,
                        batchContext.batchOffset, batchContext.lastTidOfBatchPerVms.get( vms.getIdentifier() ) );

                connectionMetadata.channel.write(connectionMetadata.writeBuffer, connectionMetadata,
                        new Coordinator.WriteCompletionHandler());

            }

        }

    }


}
