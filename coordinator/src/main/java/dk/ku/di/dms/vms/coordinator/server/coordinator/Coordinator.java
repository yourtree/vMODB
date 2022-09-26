package dk.ku.di.dms.vms.coordinator.server.coordinator;

import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.transaction.BatchContext;
import dk.ku.di.dms.vms.coordinator.server.coordinator.transaction.TransactionManagerContext;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.data_structure.KeyValueEntry;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.follower.BatchReplication;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.SERVER_TYPE;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.VMS_TYPE;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.SERVER;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.VMS;
import static dk.ku.di.dms.vms.web_common.meta.Issue.Category.UNREACHABLE_NODE;
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

    // received from program start
    private final Map<Integer, NetworkNode> starterVMSs;

    // those received from program start + those that joined later
    private final Map<String, VmsIdentifier> vmsMetadata;

    private final Map<String, TransactionDAG> transactionMap;

    private final Map<Integer, ConnectionMetadata> vmsConnectionMetadataMap;

    // the identification of this server
    private final ServerIdentifier me;

    // must update the "me" on snapshotting (i.e., committing)
    private long tid;

    // the offset of the pending batch commit (always < batchOffset)
    private long batchOffsetPendingCommit;

    // the current batch on which new transactions are being generated for
    private long batchOffset;

    // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    // defines how the batch metadata is replicated across servers
    private final BatchReplicationStrategy batchReplicationStrategy;

    // transaction requests coming from the http event loop
    private final BlockingQueue<TransactionInput> parsedTransactionRequests;

    /** serialization and deserialization of complex objects **/
    private final IVmsSerdesProxy serdesProxy;

    private final TransactionManagerContext txManagerCtx;

    // value is the batch
    BlockingQueue<KeyValueEntry<VmsIdentifier, Long>> transactionInputsToResend;

    private final Map<Integer, ConnectToVmsProtocol> connectToVmsProtocolMap;

    private final CountDownLatch connectVmsBarrier;

    private final AtomicBoolean scheduleBatchCommit;

    public Coordinator(
                       ExecutorService taskExecutor,

                       Map<Integer, ServerIdentifier> servers,
                       Map<Integer, ConnectionMetadata> serverConnectionMetadataMap,

                       Map<Integer, NetworkNode> startersVMSs,
                       Map<String, TransactionDAG> transactionMap,

                       ServerIdentifier me,
                       CoordinatorOptions options,
                       long startingTid,
                       long batchOffset,
                       BatchReplicationStrategy batchReplicationStrategy,
                       BlockingQueue<TransactionInput> parsedTransactionRequests,
                       IVmsSerdesProxy serdesProxy

    ) throws IOException {
        super();

        // network and executor
        this.group = AsynchronousChannelGroup.withThreadPool(taskExecutor);
        this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        SocketAddress address = new InetSocketAddress(me.host, me.port);
        serverSocket.bind(address);

        // task manager + (eventually) InformLeadershipTask | resendTransactionalInputEvents | general exceptions coming from completable futures
        this.taskExecutor = taskExecutor != null ? taskExecutor : Executors.newFixedThreadPool(2);

        // should come filled from election process
        this.servers = servers == null ? new ConcurrentHashMap<>() : servers;
        this.starterVMSs = startersVMSs;
        this.vmsMetadata = new HashMap<>(10);

        // connection to VMSs
        this.connectToVmsProtocolMap = new HashMap<>(10);
        this.connectVmsBarrier = new CountDownLatch(starterVMSs.size());

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
        // shared data structure
        this.parsedTransactionRequests = parsedTransactionRequests;

        // in production, it requires receiving new transaction definitions
        this.transactionMap = Objects.requireNonNull(transactionMap);
        this.txManagerCtx = new TransactionManagerContext(
                // new LinkedBlockingQueue<>(),
                new ConcurrentLinkedQueue<>(),
                new ConcurrentLinkedQueue<>() );

        // batch commit
        this.batchOffset = batchOffset;
        this.batchOffsetPendingCommit = batchOffset;
        this.batchContextMap = new ConcurrentHashMap<>();
        this.batchReplicationStrategy = batchReplicationStrategy;
        this.scheduleBatchCommit = new AtomicBoolean(false);
        this.transactionInputsToResend = new LinkedBlockingDeque<>();
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

        // setup asynchronous listener for new connections
        this.serverSocket.accept( null, new AcceptCompletionHandler());

        // only submit when there are events to react to
        // perhaps not a good idea to have this thread in a pool, since this thread will get blocked
        // BUT, it is simply about increasing the pool size with +1...
        TransactionManager txManager = new TransactionManager();

        // they can run concurrently, max 3 at a time always
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool( 1 );

        ScheduledFuture<?> batchCommitTask = scheduledExecutorService.scheduleAtFixedRate(
                this::scheduleBatchCommit, 30000, options.getBatchWindow(), TimeUnit.MILLISECONDS);

        // connect to all virtual microservices
        connectToVMSs();

        // initialize batch offset map
        this.batchContextMap.put(batchOffset, new BatchContext(batchOffset));

        while(isRunning()){

            try {

                processTransactionInputEvents();

                txManager.doAction();

                // handle batch commit task
                if(scheduleBatchCommit.get()){
                    scheduleBatchCommit.set(false);
                    runBatchCommit();
                }

                // handle other events
                if(!transactionInputsToResend.isEmpty()){
                    var kv = transactionInputsToResend.take();
                    var list = kv.getKey().transactionEventsPerBatch.get( kv.getValue() );
                    resendTransactionalInputEvents(vmsConnectionMetadataMap.get(kv.getKey().hashCode()), list);
                }

            } catch (Exception e) {
                logger.warning("Exception captured: "+e.getMessage());
            }

        }

        failSafeClose(batchCommitTask);

    }

    private class ConnectToVmsProtocol {

        private State state;
        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;
        public final CompletionHandler<Void, ConnectToVmsProtocol> connectCompletionHandler;

        public ConnectToVmsProtocol(AsynchronousSocketChannel channel){
            this.state = State.NEW;
            this.channel = channel;
            this.connectCompletionHandler = new ConnectToVmsCH();
            this.buffer = MemoryManager.getTemporaryDirectBuffer(1024);
        }

        private enum State {
            NEW,
            CONNECTED,
            PRESENTATION_SENT,
            PRESENTATION_RECEIVED,
            PRESENTATION_PROCESSED,
            CONSUMER_SET_READY,
            CONSUMER_SET_SENT
        }

        /**
         * Maybe passing the object as parameter is not necessary. it is non-static subclass,
         * so can have access to attributes...
         */
        private class ConnectToVmsCH implements CompletionHandler<Void, ConnectToVmsProtocol> {

            @Override
            public void completed(Void result, ConnectToVmsProtocol attachment) {

                attachment.state = State.CONNECTED;

                attachment.buffer.clear();
                // write presentation
                Presentation.writeServer( attachment.buffer, me, true );
                attachment.buffer.flip();

                attachment.channel.write(attachment.buffer, attachment, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, ConnectToVmsProtocol attachment) {

                        attachment.state = State.PRESENTATION_SENT;

                        // read it
                        attachment.buffer.clear();
                        attachment.channel.read(attachment.buffer, attachment, new ReadFromVmsCH());

                    }

                    @Override
                    public void failed(Throwable exc, ConnectToVmsProtocol attachment) {
                        // check if connection is still online. if so, try again
                        // otherwise, retry connection in a few minutes
                    }
                });

            }

            @Override
            public void failed(Throwable exc, ConnectToVmsProtocol attachment) {
                // queue for later attempt
                // perhaps can use scheduled task
            }
        }

        private class ReadFromVmsCH implements CompletionHandler<Integer, ConnectToVmsProtocol> {

            @Override
            public void completed(Integer result, ConnectToVmsProtocol attachment) {
                // and now read the metadata
                // any failure should be tracked so we can continue the protocol later

                attachment.buffer.clear();
                // need here otherwise the channel read handler is lost

                attachment.state = State.PRESENTATION_RECEIVED;

                // always a vms
                processVmsPresentationMessage(attachment.channel, attachment.buffer);

                attachment.state = State.PRESENTATION_PROCESSED;

                connectVmsBarrier.countDown();

            }

            @Override
            public void failed(Throwable exc, ConnectToVmsProtocol attachment) {

            }
        }

        private class ConsumerSetReady implements CompletionHandler<Integer, ConnectToVmsProtocol> {

            public ConsumerSetReady(ConnectToVmsProtocol protocol){
                protocol.state = State.CONSUMER_SET_READY;
            }

            @Override
            public void completed(Integer result, ConnectToVmsProtocol attachment) {
                attachment.state = State.CONSUMER_SET_SENT;
                attachment.buffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(attachment.buffer);
            }

            @Override
            public void failed(Throwable exc, ConnectToVmsProtocol attachment) {

            }
        }
    }

    /**
     * After a leader election, it makes more sense that
     * the leader connects to all known virtual microservices.
     */
    private void connectToVMSs() {

        // should start the protocol only if vmsMetadata is missing
        // otherwise should just connect to them

        for(NetworkNode vms : starterVMSs.values()){

            try {

                InetSocketAddress address = new InetSocketAddress(vms.host, vms.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);

                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                ConnectToVmsProtocol protocol = new ConnectToVmsProtocol(channel);

                channel.connect(address, protocol, protocol.connectCompletionHandler);

                connectToVmsProtocolMap.put(vms.hashCode(), protocol);

            } catch(IOException e){
                logger.warning("Failed to connect to a known VMS");
            }

        }

        // wait until all VMSs are online
        try {
            connectVmsBarrier.await();
        } catch (InterruptedException ignored) {
            // throw new IllegalStateException("It was not possible to start the coordinator since one or more VMSs are not operational.");
        }

        // algorithm. build consumer set dynamically
        // for each event in the transaction DAG, find the generator and receiver
        Map<Integer, Map<String, NetworkNode>> vmsConsumerSet = new HashMap<>();

        // now we can build the consumer set for each vms
        // for each internal event, find the vms who is supposed to generate and the vms
        // that receives it
        for(VmsIdentifier vms : vmsMetadata.values()){

            Map<String, NetworkNode> map = new HashMap<>(10);
            vmsConsumerSet.put(vms.hashCode(), map);

            // for each output event
            for(VmsEventSchema eventSchema : vms.outputEventSchema.values()){
                NetworkNode node = findConsumerVms( eventSchema.eventName );
                if(node != null)
                    map.put(eventSchema.eventName, node);
            }

        }

        // we need to receive the metadata from vms in order to make sure everything is correct, so we provide safety
        // the types also need to match, but the algorithm is not checking this now...

        // match output of an vms with the input of another
        // for each vms input event (not generated by the coordinator), find the vms that generated the output

        // now send the consumer set to all VMSs
        for(VmsIdentifier vms : vmsMetadata.values()){
            ConnectToVmsProtocol protocol = connectToVmsProtocolMap.get(vms.hashCode());
            protocol.buffer.clear();

            Map<String, NetworkNode> map = vmsConsumerSet.get(vms.hashCode());

            String mapStr = "";
            if(!map.isEmpty()){
                mapStr = serdesProxy.serializeMap(map);
            }
            ConsumerSet.write(protocol.buffer, mapStr);
            protocol.buffer.flip();

            // only the channel is shared. the buffer no
            vmsConnectionMetadataMap.get(vms.hashCode()).writeLock.acquireUninterruptibly();

            protocol.channel.write( protocol.buffer, protocol, protocol.new ConsumerSetReady(protocol) );

            vmsConnectionMetadataMap.get(vms.hashCode()).writeLock.release();

        }

        // new barrier? maybe not. just assume all VMSs received. otherwise they can request

        // do we need this check?
//        boolean allKnown = true;
//        for (TransactionInput.Event inputEvent : transactionInput.events) {
//            // look for the event in the topology
//            EventIdentifier event = transactionDAG.topology.get(inputEvent.name);
//            VmsIdentifier vms = vmsMetadata.get(event.vms);
//            if(vms == null) {
//                logger.info("A transaction is dropped because there is no metadata about one participating vms: "+ event.vms);
//                allKnown = false;
//                break;
//            }
//        }
//        if(!allKnown) continue;

    }

    private NetworkNode findConsumerVms(String outputEvent){

        // can be the leader or a vms
        for( VmsIdentifier vms : vmsMetadata.values() ){
            if(vms.inputEventSchema.get(outputEvent) != null){
                return vms;
            }
        }

        // assumed to be terminal? maybe yes.
        // vms is already connected to leader, no need to return coordinator
        return null;

    }

    private void failSafeClose(ScheduledFuture<?> batchCommitTask){
        // safe close
        batchCommitTask.cancel(false);
        // heartbeatTask.cancel(false); // do not interrupt given the lock management
        // parseTask.cancel(false);
//        if(!txManagerTask.isDone())
//            txManager.stop();
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
    private class VmsReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        // is it an abort, a commit response?
        // it cannot be replication because have opened another channel for that

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            // decode message by getting the first byte
            byte type = connectionMetadata.readBuffer.get(0);
            connectionMetadata.readBuffer.position(1);

            switch (type) {

                // from all terminal VMSs involved in the last batch
                case(BATCH_COMPLETE) -> {

                    // don't actually need the host and port in the payload since we have the attachment to this read operation...
                    BatchComplete.Payload response = BatchComplete.read(connectionMetadata.readBuffer);
                    txManagerCtx.batchCompleteEvents().add(response); // must have a context, i.e., what batch, the last?

                    // if one abort, no need to keep receiving
                    // actually it is unclear in which circumstances a vms would respond no... probably in case it has not received an ack from an aborted commit response?
                    // because only the aborted transaction will be rolled back
                }
                case(TX_ABORT) -> {

                    // get information of what
                    TransactionAbort.Payload response = TransactionAbort.read(connectionMetadata.readBuffer);
                    txManagerCtx.transactionAbortEvents().add(response);

                }
                case(EVENT) -> {
                    // it could be interesting to received batch events from VMSs later
                    logger.info("New event received from VMS");
                }
                default ->
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
                    starterVMSs.get(connectionMetadata.key).off();
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
                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                // right now I cannot discern whether it is a VMS or follower. perhaps I can keep alive channels from leader election?

                buffer = MemoryManager.getTemporaryDirectBuffer(1024);

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                Future<Integer> readFuture = channel.read( buffer );
                readFuture.get();

                if( !acceptConnection(channel, buffer) ) {
                    MemoryManager.releaseTemporaryDirectBuffer(buffer);
                }

            } catch(Exception e){
                // return buffer to queue
                if(channel != null && !channel.isOpen() && buffer != null){
                    MemoryManager.releaseTemporaryDirectBuffer(buffer);
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
            if(type == SERVER_TYPE){
                processServerPresentationMessage(channel, buffer);
            } else if(type == VMS_TYPE){ // vms
                processVmsPresentationMessage(channel, buffer);
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

    private void processServerPresentationMessage(AsynchronousSocketChannel channel, ByteBuffer buffer) {
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
                    newServer.hashCode(), SERVER,
                    buffer,
                    MemoryManager.getTemporaryDirectBuffer(1024),
                    channel,
                    new Semaphore(1) );

            serverConnectionMetadataMap.put( newServer.hashCode(), connectionMetadata );
            // create a read handler for this connection
            // attach buffer, so it can be read upon completion
            channel.read(buffer, connectionMetadata, new VmsReadCompletionHandler());

        }
    }

    private void processVmsPresentationMessage(AsynchronousSocketChannel channel, ByteBuffer buffer) {

        buffer.position(2);
        VmsIdentifier newVms = Presentation.readVms(buffer, serdesProxy);

        // is a default vms?
        if(starterVMSs.get( newVms.hashCode() ) != null){

            // vms reconnecting
            // newVms = vmsMetadata.get( newVms.hashCode() );

            ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( newVms.hashCode() );

            // known VMS but not yet finished the protocol
            if(connectionMetadata == null){
                connectionMetadata = new ConnectionMetadata(
                        newVms.hashCode(),
                        VMS,
                        MemoryManager.getTemporaryDirectBuffer(1024),
                        MemoryManager.getTemporaryDirectBuffer(1024),
                        channel,
                        new Semaphore(1));

                vmsConnectionMetadataMap.put(newVms.hashCode(), connectionMetadata);

            }

            // update metadata of this node
            vmsMetadata.put( newVms.getIdentifier(), newVms );

            // lock to refrain other threads from using old metadata
            connectionMetadata.writeLock.acquireUninterruptibly();

            // update channel and possibly the address
            connectionMetadata.channel = channel;
            connectionMetadata.key = newVms.hashCode();

            connectionMetadata.writeLock.release();

            // let's vms is back online from crash or is simply a new vms.
            // we need to send batch info or simply the vms assume...
            // if a vms crashed, it has lost all the events since the last batch commit, so need to resend it now

            List<TransactionEvent.Payload> list = newVms.transactionEventsPerBatch.get( newVms.lastBatch + 1 );
            if(list != null) {// avoiding creating a task for nothing
                KeyValueEntry<VmsIdentifier, Long> resendTask = new KeyValueEntry<>(newVms, newVms.lastBatch + 1);
                transactionInputsToResend.add(resendTask);
            }

            channel.read(connectionMetadata.readBuffer, connectionMetadata, new VmsReadCompletionHandler());

        } else {

            // startersVMSs.put( newVms.hashCode(), newVms );
            ConnectionMetadata connectionMetadata = new ConnectionMetadata(
                    newVms.hashCode(),
                    VMS,
                    MemoryManager.getTemporaryDirectBuffer(1024),
                    MemoryManager.getTemporaryDirectBuffer(1024),
                    channel,
                    new Semaphore(1) );

            vmsConnectionMetadataMap.put( newVms.hashCode(), connectionMetadata );

            vmsMetadata.put( newVms.getIdentifier(), newVms );

            channel.read(connectionMetadata.readBuffer, connectionMetadata, new VmsReadCompletionHandler());
        }
    }

    /**
     *
     * TODO make it a batch of events.
     * 15 | number of events | for each event { size in bytes and payload }
     *
     * A thread will execute this piece of code to liberate the "Accept" thread handler
     * This only works for input events. In case of internal events, the VMS needs to get that from the precedence VMS.
     */
    private void resendTransactionalInputEvents(ConnectionMetadata connectionMetadata,
                                                List<TransactionEvent.Payload> list){

        // assuming everything is lost, we have to resend...
        if( list != null ){
            connectionMetadata.writeLock.acquireUninterruptibly();
            for( TransactionEvent.Payload txEvent : list ){
                TransactionEvent.write( connectionMetadata.writeBuffer, txEvent);
                connectionMetadata.writeBuffer.clear();
                Future<?> task = connectionMetadata.channel.write( connectionMetadata.writeBuffer );
                try { task.get(); } catch (InterruptedException | ExecutionException ignored) {}
                connectionMetadata.writeBuffer.clear();
            }
            connectionMetadata.writeLock.release();
        }

    }

    private class EventBatchSendProtocol {

        private int idx;

        private final VmsIdentifier vms;

        private final EventBatchWriteCompletionHandler writeCompletionHandler;

        private final List<TransactionEvent.Payload> payloadList;

        private boolean terminal;

        private boolean terminalSent;

        private long batch;

        public EventBatchSendProtocol(VmsIdentifier vms, List<TransactionEvent.Payload> list, boolean terminal, long batch) {
            this.idx = 0;
            this.vms = vms;
            this.writeCompletionHandler = new EventBatchWriteCompletionHandler();
            this.payloadList = list;
            this.terminal = terminal;
            this.batch = batch;
        }

        public EventBatchSendProtocol(VmsIdentifier vms, List<TransactionEvent.Payload> list) {
            this.idx = 0;
            this.vms = vms;
            this.writeCompletionHandler = new EventBatchWriteCompletionHandler();
            this.payloadList = list;
        }

        public void init() {

            ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get(vms.hashCode());

            connectionMetadata.writeLock.acquireUninterruptibly();

            batchEvents(connectionMetadata);

            // keep sending until finished
            connectionMetadata.channel.write(connectionMetadata.writeBuffer, connectionMetadata, this.writeCompletionHandler);

        }

        private void batchEvents(ConnectionMetadata connectionMetadata){

            int bufferSize = connectionMetadata.writeBuffer.capacity();

            connectionMetadata.writeBuffer.clear();

            connectionMetadata.writeBuffer.put(BATCH_OF_EVENTS);

            // separate 4 bytes (int) for number of events in this buffer
            // connectionMetadata.writeBuffer.putInt(0);
            connectionMetadata.writeBuffer.position(5);

            // batch them all in the buffer,
            // until buffer capacity is reached or elements are all sent

            int remainingBytes = bufferSize - 1 - Integer.BYTES;
            int listSize = this.payloadList.size();

            int count = 0;

            // maybe we need a safe limit
            while(this.idx < listSize && remainingBytes > this.payloadList.get(idx).totalSize()){
                TransactionEvent.write( connectionMetadata.writeBuffer, this.payloadList.get(idx) );
                remainingBytes = remainingBytes - payloadList.get(this.idx).totalSize();
                this.idx++;
                count++;
            }

            if(this.idx == listSize && remainingBytes >= BatchCommitRequest.size){
                this.terminalSent = true;
                var batchContext = batchContextMap.get(batch);
                long lastBatchTid = batchContext.lastTidOfBatchPerVms.get( vms.getIdentifier() );
                BatchCommitRequest.write( connectionMetadata.writeBuffer, batch, lastBatchTid );
                count++;
            }

            connectionMetadata.writeBuffer.mark();
            connectionMetadata.writeBuffer.putInt(1, count);
            connectionMetadata.writeBuffer.reset();

            connectionMetadata.writeBuffer.flip();
        }

        private class EventBatchWriteCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

            @Override
            public void completed(Integer result, ConnectionMetadata connectionMetadata) {

                // do we still have events to batch?
                if(idx < payloadList.size()){
                    batchEvents(connectionMetadata);
                    connectionMetadata.channel.write(connectionMetadata.writeBuffer, connectionMetadata, this);
                } else {

                    if(terminal && !terminalSent){
                        terminalSent = true;

                        connectionMetadata.writeBuffer.clear();

                        var batchContext = batchContextMap.get(batch);
                        long lastBatchTid = batchContext.lastTidOfBatchPerVms.get( vms.getIdentifier() );

                        BatchCommitRequest.write( connectionMetadata.writeBuffer, batch, lastBatchTid );

                        connectionMetadata.writeBuffer.flip();
                        connectionMetadata.channel.write(connectionMetadata.writeBuffer, connectionMetadata, this);

                    } else {
                        connectionMetadata.writeBuffer.clear();
                        connectionMetadata.writeLock.release();
                    }
                }

            }

            @Override
            public void failed(Throwable exc, ConnectionMetadata attachment) {
                logger.warning("Failure on batching events.");
            }
        }

    }

    /**
     * Do not schedule a new batch commit if TID has not been incremented since last
     * batch commit scheduling
     */
    private void scheduleBatchCommit(){
        logger.info("Batch commit schedule spawned.");
        scheduleBatchCommit.set(true);
    }

    /**
     * Given a set of VMSs involved in the last batch
     * (for easiness can send to all of them for now)
     * send a batch request.
     *
     * Callback to start batch commit process
     *
     * we need a cut. all vms must be aligned in terms of tid
     * because the other thread might still be sending intersecting TIDs
     * e.g., vms 1 receives batch 1 tid 1 vms 2 received batch 2 tid 1
     * this is solved by fixing the batch per transaction (and all its events)
     * this is an anomaly across batches
     *
     * but another problem is that an interleaving can allow the following problem
     * commit handler issues batch 1 to vms 1 with last tid 1
     * but tx-mgr issues event with batch 1 vms 1 last tid 2
     * this can happen because the tx-mgr is still in the loop
     * this is an anomaly within a batch
     *
     * so we need synchronization to disallow the second
     *
     * obtain a consistent snapshot of last TIDs for all VMSs
     * the transaction manager must obtain the next batch inside the synchronized block
     *
     * TODO store the transactions in disk before sending
     */
    private void runBatchCommit(){

        logger.info("Batch commit run started.");

        // why do I need to replicate vmsTidMap? to restart from this point if the leader fails
        Map<String,Long> lastTidOfBatchPerVms;
        long currBatch = batchOffset;
        BatchContext currBatchContext = batchContextMap.get( currBatch );

        // a map of the last tid for each vms
        lastTidOfBatchPerVms = vmsMetadata.values().stream().collect(
                Collectors.toMap( VmsIdentifier::getIdentifier, VmsIdentifier::getLastTid ) );

        currBatchContext.seal(lastTidOfBatchPerVms);

        // increment batch offset
        batchOffset = batchOffset + 1;

        // define new batch context
        BatchContext newBatchContext = new BatchContext(batchOffset);
        batchContextMap.put( batchOffset, newBatchContext );

        logger.info("Current batch offset is "+currBatch+" and new batch offset is "+batchOffset);

        // new TIDs will be emitted with the new batch in the transaction manager

        for(VmsIdentifier vms : vmsMetadata.values()){

            var list = vms.transactionEventsPerBatch.get(currBatch);
            if(list != null) {

                EventBatchSendProtocol protocol;
                if(currBatchContext.terminalVMSs.contains(vms.vmsIdentifier)) {
                    protocol = new EventBatchSendProtocol(vms, list, true, currBatch);
                } else {
                    protocol = new EventBatchSendProtocol(vms, list);
                }
                protocol.init();
            }

        }

        // the batch commit only has progress (a safety property) the way it is implemented now when future events
        // touch the same VMSs that have been touched by transactions in the last batch.
        // how can we guarantee progress?

        if ( batchReplicationStrategy != NONE) {

            // to refrain the number of servers increasing concurrently, instead of
            // synchronizing the operation, I can simply obtain the collection first
            // but what happens if one of the servers in the list fails?
            Collection<ServerIdentifier> activeServers = servers.values();
            int nServers = activeServers.size();

            CompletableFuture<?>[] promises = new CompletableFuture[nServers];

            Set<Integer> serverVotes = Collections.synchronizedSet(new HashSet<>(nServers));

            String lastTidOfBatchPerVmsJson = serdesProxy.serializeMap(lastTidOfBatchPerVms);

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
                        BatchReplication.write(buffer, currBatch, lastTidOfBatchPerVmsJson);
                        channel.write(buffer).get();

                        buffer.clear();

                        // immediate read in the same channel
                        channel.read(buffer).get();

                        BatchReplication.BatchReplicationPayload response = BatchReplication.read(buffer);

                        buffer.clear();

                        MemoryManager.releaseTemporaryDirectBuffer(buffer);

                        // assuming the follower always accept
                        if (currBatch == response.batch()) serverVotes.add(server.hashCode());

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

    }

    /*
     * (a) Heartbeat sending to avoid followers to initiate a leader election. That can still happen due to network latency.
     *
     * Given a list of known followers, send to each a heartbeat
     * Heartbeats must have precedence over other writes, since they
     * avoid the overhead of starting a new election process in remote nodes
     * and generating new messages over the network.
     *
     * I can implement later a priority-based scheduling of writes.... maybe some Java DT can help?
     */
//    private void sendHeartbeats() {
//        logger.info("Sending vote requests. I am "+ me.host+":"+me.port);
//        for(ServerIdentifier server : servers.values()){
//            ConnectionMetadata connectionMetadata = serverConnectionMetadataMap.get( server.hashCode() );
//            if(connectionMetadata.channel != null) {
//                Heartbeat.write(connectionMetadata.writeBuffer, me);
//                connectionMetadata.writeLock.acquireUninterruptibly();
//                connectionMetadata.channel.write( connectionMetadata.writeBuffer, connectionMetadata, new WriteCompletionHandler() );
//            } else {
//                issueQueue.add( new Issue( CHANNEL_NOT_REGISTERED, server.hashCode() ) );
//            }
//        }
//    }

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
     *  processing the transaction input and creating the
     *  corresponding events
     *  the network buffering will send
     */
    private void processTransactionInputEvents(){

        // logger.info("processTransactionInputEvents spawned.");

        if(this.vmsMetadata.isEmpty()){
            logger.info("No VMS metadata received so far. Transaction input events will not be processed yet.");
            return;
        }

        int size = this.parsedTransactionRequests.size();
        if( size == 0 ){
            return;
        }

        Collection<TransactionInput> transactionRequests = new ArrayList<>(size); // threshold, for concurrent appends
        this.parsedTransactionRequests.drainTo( transactionRequests );

        for(TransactionInput transactionInput : transactionRequests){

            TransactionDAG transactionDAG = this.transactionMap.get( transactionInput.name );

            // should find a way to continue emitting new transactions without stopping this thread
            // non-blocking design
            // having the batch here guarantees that all input events of the same tid does
            // not belong to different batches

            // to allow for a snapshot of the last TIDs of each vms involved in this transaction
            long batch_ = this.batchOffset;

            // this is the only thread updating this value, so it is by design an atomic operation
            long tid_ = ++this.tid;

            // for each input event, send the event to the proper vms
            // assuming the input is correct, i.e., all events are present
            for (TransactionInput.Event inputEvent : transactionInput.events) {

                // look for the event in the topology
                EventIdentifier event = transactionDAG.topology.get(inputEvent.name);

                // get the vms
                VmsIdentifier vms = this.vmsMetadata.get(event.vms);

                // write. think about failures/atomicity later
                TransactionEvent.Payload txEvent = TransactionEvent.of(tid_, vms.lastTid, batch_, inputEvent.name, inputEvent.payload);

                // assign this event, so... what? try to send later? if a vms fail, the last event is useless, we need to send the whole batch generated so far...
                List<TransactionEvent.Payload> list = vms.transactionEventsPerBatch.computeIfAbsent(batch_, k -> new ArrayList<>(10));
                list.add(txEvent);

                // a vms, although receiving an event from a "next" batch, cannot yet commit, since
                // there may have additional events to arrive from the current batch
                // so the batch request must contain the last tid of the given vms

                // update for next transaction. this is basically to ensure VMS do not wait for a tid that will never come. TIDs processed by a vms may not be sequential
                vms.lastTid = tid_;

            }

            // add terminal to the set... so cannot be immutable when the batch context is created...
            this.batchContextMap.get(batch_).terminalVMSs.addAll( transactionDAG.terminals );

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
     *      network fluctuation is another problem...
     *
     */
    public class TransactionManager {

        public void doAction() {

            try {
                // https://web.mit.edu/6.005/www/fa14/classes/20-queues-locks/message-passing/
                byte action = !txManagerCtx.batchCompleteEvents.isEmpty() ? BATCH_COMPLETE : 0;
                action = action == 0 ? (!txManagerCtx.transactionAbortEvents.isEmpty() ? TX_ABORT : action) : action;

                //Byte action = txManagerCtx.actionQueue.poll();

                // do we have any transaction-related event?
                switch(action){
                    case BATCH_COMPLETE -> {

                        // what if ACKs from VMSs take too long? or never arrive?
                        // need to deal with intersecting batches? actually just continue emitting for higher throughput

                        BatchComplete.Payload msg = txManagerCtx.batchCompleteEvents.remove();

                        BatchContext batchContext = batchContextMap.get( msg.batch() );

                        // only if it is not a duplicate vote
                        if( batchContext.missingVotes.remove( msg.vms() ) ){

                            // making this implement order-independent, so not assuming batch commit are received in order,
                            // although they are necessarily applied in order both here and in the VMSs
                            // is the current? this approach may miss a batch... so when the batchOffsetPendingCommit finishes,
                            // it must check the batch context match to see whether it is completed
                            if( batchContext.batchOffset == batchOffsetPendingCommit && batchContext.missingVotes.size() == 0 ){

                                sendCommitRequestToVMSs(batchContext);

                                // is the next batches completed already?
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
                        for(VmsIdentifier vms : vmsMetadata.values()){

                            // don't need to send to the vms that aborted
                            if(vms.getIdentifier().equalsIgnoreCase( msg.vms() )) continue;

                            ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

                            ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(1024);

                            TransactionAbort.write(buffer, msg);
                            buffer.flip();

                            // must lock first before writing to write buffer
                            connectionMetadata.writeLock.acquireUninterruptibly();

                            try { connectionMetadata.channel.write(buffer).get(); } catch (ExecutionException ignored) {}

                            connectionMetadata.writeLock.release();

                            buffer.clear();

                            MemoryManager.releaseTemporaryDirectBuffer(buffer);

                        }


                    }

                    default -> {
                        // do nothing
                    }

                }
            } catch (InterruptedException e) {
                logger.warning("Exception.... look into that!");
                // issueQueue.add( new Issue( TRANSACTION_MANAGER_STOPPED, me.hashCode() ) );
            }

        }

        // TODO this could be asynchronously
        private void sendCommitRequestToVMSs(BatchContext batchContext){

            for(VmsIdentifier vms : vmsMetadata.values()){

                // no need to send commit request to terminals. they have already logged their state since the previous VMS in the DAG have decidedly delivered events to him
                if(batchContext.terminalVMSs.contains(vms.vmsIdentifier)) continue;

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
