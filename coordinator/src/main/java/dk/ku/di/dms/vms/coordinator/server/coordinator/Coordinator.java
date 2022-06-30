package dk.ku.di.dms.vms.coordinator.server.coordinator;

import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.election.schema.VoteRequest;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.Issue;
import dk.ku.di.dms.vms.web_common.meta.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.meta.VmsIdentifier;
import dk.ku.di.dms.vms.coordinator.server.infra.*;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchComplete;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionAbort;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchReplication;
import dk.ku.di.dms.vms.coordinator.server.schema.external.TransactionInput;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionEvent;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.web_common.buffer.BufferManager;
import dk.ku.di.dms.vms.web_common.meta.schema.control.Heartbeat;
import dk.ku.di.dms.vms.web_common.meta.schema.control.Presentation;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.coordinator.election.Constants.VOTE_REQUEST;
import static dk.ku.di.dms.vms.coordinator.server.coordinator.BatchReplicationStrategy.*;
import static dk.ku.di.dms.vms.web_common.meta.Constants.*;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.SERVER;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.VMS;
import static dk.ku.di.dms.vms.web_common.meta.Issue.Category.*;
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
    private final Map<Integer, ConnectionMetadata> serverConnectionMetadataMap;

    /** VMS data structures **/
    private final Map<Integer, VmsIdentifier> VMSs;

    private final Map<Integer, VmsConnectionMetadata> vmsConnectionMetadataMap;

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

    private final BatchReplicationStrategy batchReplicationStrategy;

    // transaction requests coming from the http event loop
    private final BlockingQueue<byte[]> transactionRequestsToParse;

    // to reuse the same collection to avoid creating collection every run
    // maybe this can be off-heap???
    private final Collection<byte[]> drainedTransactionRequests; // 100000 requests per second can be handled

    private final TransactionManagerContext txManagerCtx;

    private record TransactionManagerContext (
            // transaction manager actions
            // this provides a natural separation of tasks in the transaction manager thread. commit handling, transaction parsing, leaving the main thread free (only sending heartbeats)
            BlockingQueue<Byte> transactionManagerActionQueue,
            Queue<BatchComplete.BatchCompletePayload> batchCompleteEvents,
            Queue<TransactionAbort.TransactionAbortPayload> transactionAbortEvents
    ){}

    private final IVmsSerdesProxy serdesProxy;

    private final Map<String, TransactionDAG> transactionMap;

    public Coordinator(AsynchronousServerSocketChannel serverSocket,
                       AsynchronousChannelGroup group,
                       ExecutorService taskExecutor,
                       Map<Integer, ServerIdentifier> servers,
                       Map<Integer, VmsIdentifier> VMSs,
                       Map<Integer, ConnectionMetadata> serverConnectionMetadataMap,
                       ServerIdentifier me,
                       CoordinatorOptions options,
                       long startingTid,
                       long batchOffset,
                       BatchReplicationStrategy batchReplicationStrategy,
                       BlockingQueue<byte[]> transactionRequestsToParse,
                       IVmsSerdesProxy serdesProxy,
                       Map<String, TransactionDAG> transactionMap) {
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
        this.transactionRequestsToParse = transactionRequestsToParse; // shared data structure
        this.drainedTransactionRequests = new ArrayList<>(100000);
        this.transactionMap = Objects.requireNonNull(transactionMap); // in production, it requires receiving new transaction definitions
        this.txManagerCtx = new TransactionManagerContext(
                new LinkedBlockingQueue<>(),
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
        taskExecutor.submit( txManager );
        // txManager.run();

        // they can run concurrently, max 3 at a time always
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool( 3 );

        // callbacks
        ScheduledFuture<?> parseTask = scheduledExecutorService.schedule(this::parseAndSendTransactionInputEvents, options.getParseTimeout(), TimeUnit.MILLISECONDS);
        ScheduledFuture<?> heartbeatTask = scheduledExecutorService.schedule(this::sendHeartbeats, options.getHeartbeatTimeout() - options.getHeartbeatSlack(), TimeUnit.MILLISECONDS);
        ScheduledFuture<?> batchCommitTask = scheduledExecutorService.schedule(this::spawnBatchCommit, options.getBatchWindow(), TimeUnit.MILLISECONDS);

        // if the transaction manager thread blocks (e.g., waiting for a queue), the thread is not delivered back to the pool

        while(!isStopped()){

            try {
                issueQueue.poll(5, TimeUnit.MINUTES); // blocking, this thread should only act when necessary
                // do we have more?
//                if(issueQueue.size() > 0){
//                    /// drain to
//                }
            } catch (InterruptedException ignored) {} // not going to be interrupted by our code

        }

        // safe close
        batchCommitTask.cancel(false);
        heartbeatTask.cancel(false); // do not interrupt given the lock management
        parseTask.cancel(false);
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
                BatchComplete.BatchCompletePayload response = BatchComplete.read( connectionMetadata.readBuffer );
                txManagerCtx.batchCompleteEvents().add( response );
                txManagerCtx.transactionManagerActionQueue.add(BATCH_COMPLETE); // must have a context, i.e., what batch, the last?

                // if one abort, no need to keep receiving
                // actually it is unclear in which circumstances a vms would respond no... probably in case it has not received an ack from an aborted commit response?
                // because only the aborted transaction will be rolled back

            } else if (type == TX_ABORT){

                // get information of what
                TransactionAbort.TransactionAbortPayload response = TransactionAbort.read(connectionMetadata.readBuffer);
                txManagerCtx.transactionAbortEvents().add( response );
                txManagerCtx.transactionManagerActionQueue.add(TX_ABORT);

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
                    VMSs.get(connectionMetadata.key).active = false;
                } else {
                    servers.get(connectionMetadata.key).active = false;
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

                channel.setOption(TCP_NODELAY, true); // true disable the nagle's algorithm. not useful to have coalescence of messages in election'
                channel.setOption(SO_KEEPALIVE, true); // better to keep alive now, independently if that is a VMS or follower

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

        private boolean acceptConnection(AsynchronousSocketChannel channel, ByteBuffer buffer){

            // message identifier
            byte messageIdentifier = buffer.get(0);

            if(messageIdentifier == VOTE_REQUEST){
                // so I am leader, and I respond with a leader request to this new node
                // taskExecutor.submit( new ElectionWorker.WriteTask( LEADER_REQUEST, server ) );
                // would be better to maintain the connection open.....
                buffer.clear();
                ServerIdentifier serverRequestingVote = VoteRequest.read(buffer);
                taskExecutor.submit( new InformLeadershipTask(serverRequestingVote) );
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
                    connectionMetadata.writeLock.lock();

                    // update metadata of this node
                    servers.put( newServer.hashCode(), newServer );

                    connectionMetadata.channel = channel;

                    connectionMetadata.writeLock.unlock();

                } else { // no need for locking here

                    servers.put( newServer.hashCode(), newServer );

                    ConnectionMetadata connectionMetadata = new ConnectionMetadata( newServer.hashCode(), SERVER, buffer, BufferManager.loanByteBuffer(), channel, new ReentrantLock() );
                    serverConnectionMetadataMap.put( newServer.hashCode(), connectionMetadata );
                    // create a read handler for this connection
                    // attach buffer, so it can be read upon completion
                    channel.read(buffer, connectionMetadata, new ReadCompletionHandler());

                }
            } else if(type == 1){ // vms

                VmsIdentifier newVms = Presentation.readVms(buffer, serdesProxy);

                if(VMSs.get( newVms.name.hashCode() ) != null){
                    // vms reconnecting

                    VmsConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( newVms.hashCode() );

                    // lock to refrain other threads from using old metadata
                    connectionMetadata.writeLock.lock();

                    // update metadata of this node
                    VMSs.put( newVms.name.hashCode(), newVms );

                    // update channel and possibly the address
                    connectionMetadata.channel = channel;
                    connectionMetadata.key = newVms.hashCode();

                    connectionMetadata.writeLock.unlock();

                    // let's vms is back online from crash or is simply a new vms.
                    // we need to send batch info or simply the vms assume...
                    // if a vms crashed, it has lost all the events since the last batch commit, so need to resend it now
                    if(connectionMetadata.transactionEventsPerBatch.get( newVms.lastBatch + 1 ) != null) // avoiding creating a thread for nothing
                        taskExecutor.submit( () -> resendTransactionalInputEvents(connectionMetadata, newVms) );

                    channel.read(buffer, connectionMetadata, new ReadCompletionHandler());

                } else {
                    VMSs.put( newVms.name.hashCode(), newVms );
                    VmsConnectionMetadata connectionMetadata = new VmsConnectionMetadata( newVms.hashCode(), buffer, BufferManager.loanByteBuffer(), channel, new ReentrantLock() );
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
     * Task for informing the server running for leader that a leader is already established
     * We would no longer need to establish connection in case the {@link dk.ku.di.dms.vms.coordinator.election.ElectionWorker}
     * maintains the connections.
     */
    private class InformLeadershipTask implements Runnable {

        private final ServerIdentifier connectTo;

        public InformLeadershipTask(ServerIdentifier connectTo){
            this.connectTo = connectTo;
        }

        @Override
        public void run() {

            try {

                InetSocketAddress address = new InetSocketAddress(connectTo.host, connectTo.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, false);

                channel.connect(address).get();

                ByteBuffer buffer = ByteBuffer.allocate(128);

                LeaderRequest.write(buffer, me);

                channel.write(ByteBuffer.wrap(buffer.array())).get();

                if(channel.isOpen()) {
                    channel.close();
                }

            } catch(Exception ignored){}

        }

    }

    /**
     * A thread will execute this piece of code to liberate the "Accept" thread handler
     * This only works for input events. In case of internal events, the VMS needs to get that from the precedence VMS.
     */
    private void resendTransactionalInputEvents(VmsConnectionMetadata connectionMetadata, VmsIdentifier vmsIdentifier){

        // is this usually the last batch...?
        List<TransactionEvent.Payload> list = connectionMetadata.transactionEventsPerBatch.get( vmsIdentifier.lastBatch + 1 );

        // assuming everything is lost, we have to resend...
        if( list != null ){
            connectionMetadata.writeLock.lock();
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
                    Collectors.toMap( VmsIdentifier::getName, VmsIdentifier::getLastTid ) );

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

            if(!server.active) continue;
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
                defaultLogError( UNREACHABLE_NODE, server );
                return null;
            }, taskExecutor);
            i++;
        }

        if ( batchReplicationStrategy == ONE ){
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
     * After a leader election, it makes more sense that
     * the leader connects to all known virtual microservices.
     */
    private void connectToVMSs(){

        for(VmsIdentifier vms : VMSs.values()){

            try {

                InetSocketAddress address = new InetSocketAddress(vms.host, vms.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                try {
                    channel.connect(address).get();

                    VmsConnectionMetadata connMetadata = new VmsConnectionMetadata(vms.hashCode(),
                            BufferManager.loanByteBuffer(), BufferManager.loanByteBuffer(), channel, new ReentrantLock());
                    vmsConnectionMetadataMap.put(vms.hashCode(), connMetadata);

                    // write presentation
                    Presentation.writeServer( connMetadata.writeBuffer, this.me );

                    connMetadata.channel.write( connMetadata.writeBuffer ).get();

                    connMetadata.writeBuffer.clear();

                    // now read the presentation message of a vms...
                    channel.read(connMetadata.readBuffer).get();

                    VmsIdentifier vmsIdentifier = Presentation.readVms( connMetadata.readBuffer, serdesProxy );

                    VMSs.put( vmsIdentifier.hashCode(), vmsIdentifier );

                    channel.read(connMetadata.readBuffer, connMetadata, new ReadCompletionHandler());

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }


            } catch(IOException ignored){ }

        }

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
                connectionMetadata.writeLock.lock();
                connectionMetadata.channel.write( connectionMetadata.writeBuffer, connectionMetadata, new WriteCompletionHandler() );
            } else {
                issueQueue.add( new Issue( CHANNEL_NOT_REGISTERED, server ) );
            }
        }
    }

    /**
     * Allows to reuse the thread pool assigned to socket to complete the writing
     * That refrains the main thread and the TransactionManager to block, thus allowing its progress
     */
    private static final class WriteCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.clear();
            connectionMetadata.writeLock.unlock();
        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.clear();
            connectionMetadata.writeLock.unlock();
        }

    }

    /**
     * This task assumes the channels are already established
     * Cannot have two threads writing to the same channel at the same time
     * A transaction manager is responsible for assigning TIDs to incoming transaction requests
     * This task also involves making sure the writes are performed successfully
     * A writer manager is responsible for defining strategies, policies, safety guarantees on
     * writing concurrently to channels.
     */
    private class TransactionManager extends StoppableRunnable {

        @Override
        public void run() {

            while(!isStopped()){

                try {
                    // https://web.mit.edu/6.005/www/fa14/classes/20-queues-locks/message-passing/
                    byte action = txManagerCtx.transactionManagerActionQueue().take();

                    // do we have any transaction-related event?
                    switch(action){
                        case BATCH_COMPLETE -> {

                            // what if ACKs from VMSs take too long? or never arrive?
                            // need to deal with intersecting batches? actually just continue emitting for higher throughput

                            BatchComplete.BatchCompletePayload msg = txManagerCtx.batchCompleteEvents().remove();

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

                            TransactionAbort.TransactionAbortPayload msg = txManagerCtx.transactionAbortEvents.remove();

                            // can reuse the same buffer since the message does not change across VMSs like the commit request
                            for(VmsIdentifier vms : VMSs.values()){

                                // don't need to send to the vms that aborted
                                if(vms.name.equalsIgnoreCase( msg.vms() )) continue;

                                VmsConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

                                ByteBuffer buffer = BufferManager.loanByteBuffer();

                                TransactionAbort.write(buffer, msg);

                                // must lock first before writing to write buffer
                                connectionMetadata.writeLock.lock();

                                try { connectionMetadata.channel.write(buffer).get(); } catch (ExecutionException ignored) {}

                                connectionMetadata.writeLock.unlock();

                                buffer.clear();

                                BufferManager.returnByteBuffer(buffer);

                            }


                        }
                    }
                } catch (InterruptedException e) {
                    issueQueue.add( new Issue( TRANSACTION_MANAGER_STOPPED, me ) );
                }

            }

        }

        // this could be asynchronously
        private void sendCommitRequestToVMSs(BatchContext batchContext){

            for(VmsIdentifier vms : VMSs.values()){

                VmsConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

                // must lock first before writing to write buffer
                connectionMetadata.writeLock.lock();

                // having more than one write buffer does not help us, since the connection is the limitation (one writer at a time)
                BatchCommitRequest.write( connectionMetadata.writeBuffer,
                        batchContext.batchOffset, batchContext.lastTidOfBatchPerVms.get( vms.getName() ) );

                connectionMetadata.channel.write(connectionMetadata.writeBuffer, connectionMetadata, new WriteCompletionHandler());

            }

        }

    }

    /**
     *  Should read in a proportion that matches the batch and heartbeat window, otherwise
     *  how long does it take to process a batch of input transactions?
     *  instead of trying to match the rate of processing, perhaps we can create read tasks
     */
    private void parseAndSendTransactionInputEvents(){

        if (transactionRequestsToParse.size() == 0) {
            return;
        }

        // the payload received in bytes is json format

        transactionRequestsToParse.drainTo(drainedTransactionRequests);

        List<TransactionInput> parsedTransactionRequests = new ArrayList<>( drainedTransactionRequests.size() );

        // do not send another until the last has been completed, so we can better adjust the rate of parsing
        for (byte[] drainedElement : drainedTransactionRequests) {
            String json = new String(drainedElement);

            // must also check whether the event is correct, that is, contains all events
            try {
                TransactionInput transactionInput = serdesProxy.deserialize(json, TransactionInput.class);
                // order by name, since we guarantee the topology input events are ordered by name
                transactionInput.events.sort( Comparator.comparing(o -> o.name) );
                parsedTransactionRequests.add(transactionInput);
            } catch(Exception ignored) {} // simply ignore, probably problem in the JSON
        }

        // clean for next iteration
        drainedTransactionRequests.clear();

        for(TransactionInput transactionInput : parsedTransactionRequests){

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
                VmsConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

                // we could employ deterministic writes to the channel, that is, an order that would not require locking for writes
                // we could possibly open a channel per write operation, but... what are the consequences of too much opened connections?
                connectionMetadata.writeLock.lock();

                // write. think about failures/atomicity later
                TransactionEvent.Payload txEvent = TransactionEvent.write(connectionMetadata.writeBuffer, tid_, vms.lastTid, batch_, inputEvent.name, inputEvent.payload);

                // assign this event, so... what? try to send later? if a vms fail, the last event is useless, we need to send the whole batch generated so far...
                List<TransactionEvent.Payload> list = connectionMetadata.transactionEventsPerBatch.get(batch_);
                if (list == null ){
                    connectionMetadata.transactionEventsPerBatch.put(batch_, new ArrayList<>());
                } else {
                    list.add(txEvent);
                }

                // a vms, although receiving an event from a "next" batch, cannot yet commit, since
                // there may have additional events to arrive from the current batch
                // so the batch request must contain the last tid of the given vms

                // update for next transaction
                vms.lastTid = tid_;

                connectionMetadata.channel.write(connectionMetadata.writeBuffer,
                        connectionMetadata, new WriteCompletionHandler());

            }

            // add terminal to the set... so cannot be immutable when the batch context is created...
            batchContextMap.get(batch_).terminalVMSs.addAll( transactionDAG.terminals );

        }

    }

}
