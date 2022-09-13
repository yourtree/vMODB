package dk.ku.di.dms.vms.coordinator.server.coordinator;

import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.transaction.TransactionManager;
import dk.ku.di.dms.vms.coordinator.server.coordinator.transaction.TransactionManagerContext;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.buffer.BufferManager;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.network.NetworkRunnable;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

//import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.ConnectionMetadata.NodeType.SERVER;
import static dk.ku.di.dms.vms.modb.common.schema.network.ConnectionMetadata.NodeType.VMS;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Issue.Category.*;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.VMS;

/**
 * Class that encapsulates all logic related to issuing of
 * batch commits, transaction aborts, ...
 *
 * Actually no!
 * This class handles all infrastructure-related tasks to alleviate the core task,
 * which is processing transactions, commits, and aborts.
 *
 * This class deals with runtime, network, connections, node errors, making sure VMSs are correct
 * Encapsulate run options
 * Monitor how healthy the transaction processing is
 *
 * manages the timing of the heartbeat (priority queue, 1 - heartbeat, 2 - batch commit and abort, 3 - transaction)
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

    private final Map<Integer, ConnectionMetadata> vmsConnectionMetadataMap;

    // the identification of this server
    private final ServerIdentifier me;

    /*
     * Transaction requests coming from the http event loop
     * Data structure shared with transaction manager
     */
    private final ConcurrentLinkedQueue<TransactionInput> parsedTransactionRequests;

    /** serialization and deserialization of complex objects **/
    private final IVmsSerdesProxy serdesProxy;

    private final TransactionManagerContext txManagerCtx;

    public Coordinator(AsynchronousServerSocketChannel serverSocket,
                       AsynchronousChannelGroup group,
                       ExecutorService taskExecutor,
                       Map<Integer, ServerIdentifier> servers,
                       Map<Integer, VmsIdentifier> VMSs,
                       Map<String, TransactionDAG> transactionMap,
                       Map<Integer, List<VmsIdentifier>> vmsDependenceSet, // built from the transactionMap
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
        // this.batchCommitLock = new Object();

        // might come filled from election process
        this.serverConnectionMetadataMap = serverConnectionMetadataMap == null ? new HashMap<>() : serverConnectionMetadataMap;
        this.vmsConnectionMetadataMap = new HashMap<>();
        this.me = me;

        // infra
        this.serdesProxy = serdesProxy;

        // coordinator options
        this.options = options;

        // transactions
//        this.tid = startingTid;
//        this.parsedTransactionRequests = parsedTransactionRequests; // shared data structure
//        this.transactionMap = Objects.requireNonNull(transactionMap); // in production, it requires receiving new transaction definitions
//        this.vmsConsumerSet = vmsDependenceSet;
//        this.txManagerCtx = new TransactionManagerContext(
//                new LinkedBlockingQueue<byte>(),
//                new ConcurrentLinkedQueue<>(),
//                new ConcurrentLinkedQueue<>() );

        // batch commit
//        this.batchOffset = batchOffset;
//        this.batchOffsetPendingCommit = batchOffset;
//        this.batchContextMap = new ConcurrentHashMap<>();
//        this.batchReplicationStrategy = batchReplicationStrategy;
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
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool( options.getScheduledTasksThreadPoolSize() );

        // this must not be executed in batches, must execute non stop
        // ScheduledFuture<?> processTask = scheduledExecutorService.scheduleWithFixedDelay(this::processTransactionInputEvents, 0L, options.getParseTimeout(), TimeUnit.MILLISECONDS);

        // callbacks
        // TODO put in the UDP server ScheduledFuture<?> heartbeatTask = scheduledExecutorService.scheduleAtFixedRate(this::sendHeartbeats, 0L, options.getHeartbeatTimeout() - options.getHeartbeatSlack(), TimeUnit.MILLISECONDS);
        ScheduledFuture<?> batchCommitTask = scheduledExecutorService.scheduleAtFixedRate(this::spawnBatchCommit, 0L, options.getBatchWindow(), TimeUnit.MILLISECONDS);

        // if the transaction manager thread blocks (e.g., waiting for a queue), the thread is not delivered back to the pool

        while(isRunning()){

            try {
                // issueQueue.poll(5, TimeUnit.MINUTES); // blocking, this thread should only act when necessary
                issueQueue.take();
            } catch (InterruptedException ignored) {} // not going to be interrupted by our code

        }

        failSafeClose(batchCommitTask, heartbeatTask, txManagerTask, txManager);

    }

    private void failSafeClose(ScheduledFuture<?> batchCommitTask, ScheduledFuture<?> heartbeatTask,
                               Future<?> txManagerTask, TransactionManager txManager){
        // safe close
        batchCommitTask.cancel(false);
        heartbeatTask.cancel(false); // do not interrupt given the lock management
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

                if(VMSs.get( newVms.hashCode() ) != null){
                    // vms reconnecting

                    ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( newVms.hashCode() );

                    // lock to refrain other threads from using old metadata
                    connectionMetadata.writeLock.lock();

                    // update metadata of this node
                    VMSs.put( newVms.hashCode(), newVms );

                    // update channel and possibly the address
                    connectionMetadata.channel = channel;
                    connectionMetadata.key = newVms.hashCode();

                    connectionMetadata.writeLock.unlock();

                    // let's vms is back online from crash or is simply a new vms.
                    // we need to send batch info or simply the vms assume...
                    // if a vms crashed, it has lost all the events since the last batch commit, so need to resend it now
                    if(connectionMetadata.transactionEventsPerBatch.get( newVms.lastBatch + 1 ) != null) {// avoiding creating a thread for nothing
                         taskExecutor.submit(() -> resendTransactionalInputEvents(connectionMetadata, newVms));
                    }

                    channel.read(buffer, connectionMetadata, new ReadCompletionHandler());

                } else {
                    VMSs.put( newVms.hashCode(), newVms );
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
     * After a leader election, it makes more sense that
     * the leader connects to all known virtual microservices.
     */
    private void connectToVMSs(){

        for(VmsIdentifier vms : VMSs.values()){

            try {

                InetSocketAddress address = new InetSocketAddress(vms.host, vms.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                withDefaultSocketOptions(channel);

                try {
                    channel.connect(address).get();

                    ConnectionMetadata connMetadata = new ConnectionMetadata(vms.hashCode(),
                            BufferManager.loanByteBuffer(), BufferManager.loanByteBuffer(),
                            channel, new ReentrantLock());
                    vmsConnectionMetadataMap.put(vms.hashCode(), connMetadata);

                    // get dependence set of given VMS
                    List<VmsIdentifier> consumerSet = vmsConsumerSet.get( vms.hashCode() );

                    // why not sending only the host+address? the vms must be able to identify the cross-cutting constraints
                    String consumerSetJson = serdesProxy.serializeList(consumerSet);

                    // write presentation
                    Presentation.writeServer( connMetadata.writeBuffer, this.me, consumerSetJson );

                    connMetadata.channel.write( connMetadata.writeBuffer ).get();

                    connMetadata.writeBuffer.clear();

                    // now read the presentation message of a vms...
                    channel.read(connMetadata.readBuffer).get();

                    VmsIdentifier vmsIdentifier = Presentation.readVms( connMetadata.readBuffer, serdesProxy );

                    VMSs.put( vmsIdentifier.hashCode(), vmsIdentifier );

                    // start reading task
                    channel.read(connMetadata.readBuffer, connMetadata, new ReadCompletionHandler());

                } catch (InterruptedException | ExecutionException e) {
                    issueQueue.add( new Issue( CANNOT_READ_FROM_NODE, vms ) ); // or write to
                }


            } catch(IOException ignored){ }

        }

    }

}
