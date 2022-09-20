//package dk.ku.di.dms.vms.coordinator.election;
//
//import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
//import dk.ku.di.dms.vms.coordinator.election.schema.VoteRequest;
//import dk.ku.di.dms.vms.coordinator.election.schema.VoteResponse;
//import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
//import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
//import dk.ku.di.dms.vms.web_common.connection.ConnectionMetadata;
//import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
//import dk.ku.di.dms.vms.web_common.network.NetworkRunnable;
//import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
//
//import java.io.IOException;
//import java.net.*;
//import java.nio.ByteBuffer;
//import java.nio.channels.DatagramChannel;
//import java.util.Map;
//import java.util.Objects;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
//
///**
// * An election task is a thread that encapsulates all subtasks (i.e., threads)
// * necessary to complete a leader election. Only after this thread finishes,
// * a server can act as a leader or follower.
// *
// * Define leader based on highest offset and timestamp
// * while election is not defined and election timeout has not timed out, continue
// *
// * We assume the nodes are fixed. Later we revisit this choice.
// * TODO Cluster membership management (e.g., removing nodes, replacing nodes)
// *
// * Protocol SCTP is maybe a better fit for leader election since it is message-oriented, rather than stream oriented
// * On the other hand, the UDP allows multicast, which is good for leader election (sending messages to all nodes
// * by design instead of iterating over the nodes to send individual messages)
// */
//public final class ElectionWorkerUdp extends NetworkRunnable {
//
//    private final DatagramChannel datagramChannel;
//
//    private volatile int state;
//    public static final int NEW          = 0;
//    public static final int CANDIDATE    = 1; // running the protocol
//    public static final int LEADER       = 2; // has received the ACKs from a majority
//    public static final int FOLLOWER     = 3;
//
//    // general tasks, like sending info to VMSs and other servers
//    private final ExecutorService taskExecutor;
//
//    // the identification of this server
//    private final ServerIdentifier me;
//
//    // can be == me
//    // only one thread modifying it, no need for atomic reference
//    private volatile ServerIdentifier leader;
//
//    // even though we can start with a known number of servers, their payload may have changed after a crash
//    private final Map<Integer, ServerIdentifier> servers;
//
//    private final Map<Integer, ConnectionMetadata> connectionMetadataMap;
//
//    // every replica is part of this channel
//    // private final MulticastChannel multicastChannel;
//
//    // a bounded time in which a leader election must occur, otherwise it should restart. in milliseconds
//    // only one thread modifying, no need to use AtomicLong
//    private volatile long timeout;
//
//    // https://stackoverflow.com/questions/3786825/volatile-boolean-vs-atomicboolean
//    private final AtomicBoolean voted;
//
//    private final Object responseMapLock = new Object();
//    private final Map<Integer, VoteResponse.Payload> responses;
//
//    // making sure read-after-write atomicity for writers
//    private final AtomicInteger opN = new AtomicInteger(0);
//    // volatile to make sure all threads don't read from their cpu cache
//    private volatile int N; // only gets updated when a round terminates
//
//    private final ElectionOptions options;
//
//    // for broadcaster
//    private final BlockingQueue<byte> broadcastMessagesToSend;
//
//    // for simple sender
//    private final BlockingQueue<VoteMessageContext> voteMessagesToSend;
//
//    private static class VoteMessageContext {
//        byte type; // vote or response
//        ServerIdentifier source;
//        ServerIdentifier target;
//        boolean response;
//
//        public VoteMessageContext(byte type, ServerIdentifier target, boolean response) {
//            this.type = type;
//            this.target = target;
//            this.response = response;
//        }
//
//        public VoteMessageContext(ServerIdentifier source, ServerIdentifier target) {
//            this.type = VOTE_REQUEST;
//            this.source = source;
//            this.target = target;
//        }
//    }
//
//    public ElectionWorkerUdp(
//                          ExecutorService taskExecutor,
//                          ServerIdentifier me,
//                          Map<Integer, ServerIdentifier> servers,
//                          ElectionOptions options) throws IOException {
//        super();
//        this.state = NEW;
//        this.taskExecutor = taskExecutor;
//        this.me = me;
//        this.servers = servers;
//        this.connectionMetadataMap = new ConcurrentHashMap<>();
//
//        this.voted = new AtomicBoolean(false);
//
//        this.N = servers.size();
//        this.responses = new ConcurrentHashMap<>();
//
//        this.options = options;
//
//        // communication channels with threads
//        this.broadcastMessagesToSend = new LinkedBlockingQueue<byte>();
//        this.voteMessagesToSend = new LinkedBlockingQueue<>();
//
//        this.datagramChannel = DatagramChannel
//                .open(StandardProtocolFamily.INET)
//                .setOption(StandardSocketOptions.SO_REUSEADDR, true)
//                .bind(new InetSocketAddress(me.host, me.port))
//                ;
//
//        this.datagramChannel.configureBlocking(false);
//
//    }
//
//    private void runRound(){
//
//        // start round sending vote requests
//        broadcastMessagesToSend.add( VOTE_REQUEST );
//
//        int state_ = state;
//
//        // force read from memory only once
//        long timeout_ = timeout;
//
//        /* while majority cannot be established, we cannot proceed safely */
//        long elapsed = 0L;
//        long startTime = System.currentTimeMillis();
//        while(elapsed < timeout_ && state_ == CANDIDATE){
//            try { Thread.sleep(1000); } catch (InterruptedException ignored) {} // this can make messages not being handled. it is the timeout making this happen
//            elapsed = System.currentTimeMillis() - startTime;
//            state_ = state;
//        }
//
//        // round is over, responses received no longer hold
//        // this is the same in all servers
//        if(state_ == CANDIDATE){
//            resetVoteResponses();// data races may still allow old votes to be computed
//            voted.set(false); // make sure the next round this server is able to vote
//        }
//
//        logger.info("Event loop has finished. I am "+me.host+":"+me.port+" and my state is "+state_);
//
//    }
//
//    /**
//     *  Have to cancel all read completion handlers and queued
//     *  writes after leader is elected
//     */
//    @Override
//    public void run() {
//
//        logger.info("Initializing election round. I am "+me.host+":"+me.port);
//
//        initialize();
//
//        long timeout_;
//
//        while(isRunning()){
//
//            // the round is an abstraction to avoid a vote given from holding forever (e.g., the requesting node may have crashed)
//            runRound();
//
//            if(state == CANDIDATE) {
//                // define a new delta since defining a leader is taking too long
//                // increment since nothing has been defined
//                timeout_ = timeout; // avoid two reads from memory
//                timeout = (long) (timeout_ + (timeout_ * options.getRoundDeltaIncrease()));
//            } else {
//                // break loop since the election is over
//                break;
//            }
//        }
//
//        // signal the server
//        signal.add(FINISHED);
//
//    }
//
//    private void initialize() {
//
//        this.state = CANDIDATE;
//
//        // then start the broadcaster and vote response threads
//        taskExecutor.submit( new ReadHandler() );
//        taskExecutor.submit( new Broadcaster() );
//        taskExecutor.submit( new SimpleSender() );
//
//        timeout = options.getInitRoundTimeout();
//
//    }
//
//    private void resetVoteResponses(){
//        synchronized (responseMapLock) {
//            this.responses.clear();
//            // do we have any new node?
//            this.N += opN.get();
//        }
//        this.opN.set(0);
//    }
//
//    /**
//     * Thread responsible for handling messages related to leader election process.
//     * I cannot set up the timeout here because I may spawn more than a thread to
//     * handle messages. The timeout must be external to this thread, it is, the
//     * thread that "owns" this one.
//     * https://blog.gceasy.io/2021/02/24/java-threads-may-not-be-memory-efficient/
//     * Single thread, so no need to deal with data races.
//     */
//    private class ReadHandler extends StoppableRunnable {
//
//        @Override
//        public void run() {
//
//            ByteBuffer buf = MemoryManager.getTemporaryDirectBuffer(1024);
//
//            while(isRunning()) {
//                try {
//                    SocketAddress res = datagramChannel.receive(buf);
//
//                    if(res != null){
//                        // new one, has not been running from the start
//                        int resHashCode = res.hashCode();
//                        if(connectionMetadataMap.get(resHashCode) == null){
//                            connectionMetadataMap.put(res.hashCode(),
//                                    new ConnectionMetadata(resHashCode,
//                                            ConnectionMetadata.NodeType.SERVER, null,
//                                            MemoryManager.getTemporaryDirectBuffer(1024) ) );
//                        }
//                        completed(buf);
//                    }
//                } catch (IOException ignored) { }
//                finally {
//                    buf.clear();
//                }
//            }
//        }
//
//        public void completed(ByteBuffer readBuffer) {
//
//            byte messageIdentifier = readBuffer.get();
//
//            logger.info("Message read. I am "+me.host+":"+me.port+" identifier is "+messageIdentifier);
//
//            switch (messageIdentifier) {
//                case VOTE_RESPONSE -> {
//
//                    VoteResponse.Payload payload = VoteResponse.read(readBuffer);
//
//                    logger.info("Vote response received: "+ payload.response +". I am " + me.host + ":" + me.port);
//
//                    int serverId = Objects.hash(payload.host, payload.port);
//
//                    // is it a yes? add to responses
//                    if (!payload.response) {
//                        break;
//                    }
//
//                    synchronized (responseMapLock) { // the responses are perhaps being reset
//                        if (!responses.containsKey(serverId)) { // avoid duplicate vote
//
//                            responses.put(serverId, payload);
//
//                            // do I have the majority of votes?
//                            // !voted prevent two servers from winning the election... but does not prevent
//                            if (!voted.get() && responses.size() + 1 > (N / 2)) {
//                                logger.info("I am leader. I am " + me.host + ":" + me.port);
//                                leader = me; // only this thread is writing
//
//                                broadcastMessagesToSend.add(LEADER_REQUEST);
//
//                                state = LEADER;
//                            }
//
//                        }
//                    }
//
//                }
//
//                case VOTE_REQUEST -> {
//
//                    logger.info("Vote request received. I am " + me.host + ":" + me.port);
//
//                    ServerIdentifier serverRequestingVote = VoteRequest.read(readBuffer);
//
//                    if (voted.get()) {
//                        // taskExecutor.submit(new Broadcaster(VOTE_RESPONSE, serverRequestingVote, false));
//                        logger.info("Vote not granted, already voted. I am " + me.host + ":" + me.port);
//                    } else {
//
//                        // TO AVOID TWO (OR MORE) LEADERS!!!
//                        // if a server is requesting a vote, it means this server is
//                        // in another round, so I need to remove its vote from my (true) responses
//                        // because this server may give a vote to another server
//                        // in this case I will send a vote request again
//                        // (could simply remove it, but would necessarily force another round,
//                        // while resending may still allow this sender to win)
//                        boolean previousVoteReceived = false;
//                        synchronized (responseMapLock) { // the responses are perhaps being reset, cannot count on this vote anymore
//                            if (responses.get(serverRequestingVote.hashCode()) != null) {
//                                responses.remove( serverRequestingVote.hashCode() );
//                                previousVoteReceived = true;
//                            }
//                        }
//
//                        if (serverRequestingVote.lastOffset > me.lastOffset) {
//                            // grant vote
//                            voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, true ) );
//                            voted.set(true);
//                            logger.info("Vote granted. I am " + me.host + ":" + me.port);
//                        } else if (serverRequestingVote.lastOffset < me.lastOffset) {
//                            voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, false ) );
//                            logger.info("Vote not granted. I am " + me.host + ":" + me.port);
//                        } else { // equal
//
//                            if (serverRequestingVote.hashCode() > me.hashCode()) {
//                                // grant vote
//                                voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, true ) );
//                                voted.set(true);
//                                logger.info("Vote granted. I am " + me.host + ":" + me.port);
//                            } else {
//                                voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, false ) );
//                                logger.info("Vote not granted. I am " + me.host + ":" + me.port);
//                            }
//
//                        }
//
//                        // this is basically attempt to refresh the vote in case of intersecting distinct rounds in different servers
//                        if(!voted.get() && previousVoteReceived){
//                            voteMessagesToSend.add( new VoteMessageContext( me, serverRequestingVote ) );
//                        }
//
//                    }
//                }
//
//                case LEADER_REQUEST -> {
//                    logger.info("Leader request received. I am " + me.host + ":" + me.port);
//                    LeaderRequest.LeaderRequestPayload leaderRequest = LeaderRequest.read(readBuffer);
//                    leader = servers.get(leaderRequest.hashCode());
//                    state = FOLLOWER;
//                }
//            }
//
//        }
//
//
//        // logger.info("Message handler is finished. I am "+me.host+":"+me.port);
//
//    }
//
//
//
//    /**
//     * Responsible for sending voting and leader requests
//     * Each action in the action queue means the respective message
//     * must be sent to all nodes
//     */
//    private class Broadcaster extends StoppableRunnable {
//
//        @Override
//        public void run() {
//
//            ByteBuffer buf = MemoryManager.getTemporaryDirectBuffer(1024);
//
//            while(isRunning()) {
//
//                try {
//
//                    byte messageType = broadcastMessagesToSend.take();
//
//                    // now send the request
//                    if (messageType == VOTE_REQUEST) {
//
//                        for( var entry : servers.entrySet()) {
//
//                            datagramChannel.send(buf, entry.getValue().socketAddress);
//                        }
//                        buf.clear();
//
//                        VoteRequest.write(buf, me);
//                    } else if (messageType == LEADER_REQUEST) {
//                        LeaderRequest.write(buf, me);
//                    } else {
//                        logger.warning("Error on write. I am " + me.host + ":" + me.port + " message type is " + messageType);
//                        buf.clear();
//                    }
//
//
//
//                } catch (Exception ignored) {
//                    logger.warning("Error on write. I am " + me.host + ":" + me.port + " message type is ...");
//                }
//
//            }
//
//        }
//
//    }
//
//    /**
//     * Send individual messages to a particular node
//     */
//    private class SimpleSender extends StoppableRunnable {
//
//        @Override
//        public void run() {
//
//            ByteBuffer buf = MemoryManager.getTemporaryDirectBuffer(1024);
//
//            while (isRunning()){
//
//                try {
//                    VoteMessageContext msgContext = voteMessagesToSend.take();
//
//                    if(msgContext.type == VOTE_RESPONSE){
//
//                        ConnectionMetadata connMeta = connectionMetadataMap.get( msgContext.target.hashCode() );
//
//                        VoteResponse.write(connMeta.writeBuffer, me, msgContext.response);
//
//                        datagramChannel.send( connMeta.writeBuffer, msgContext.target.socketAddress );
//
//                        connMeta.writeBuffer.clear();
//
//                    } else if (msgContext.type == VOTE_REQUEST){
//
//                        ConnectionMetadata connMeta = connectionMetadataMap.get( msgContext.target.hashCode() );
//
//                        VoteRequest.write(connMeta.writeBuffer, me);
//
//                        datagramChannel.send( connMeta.writeBuffer, msgContext.target.socketAddress );
//
//                        connMeta.writeBuffer.clear();
//                    }
//
//                } catch (InterruptedException | IOException ignored) { }
//
//            }
//
//        }
//    }
//
//    public int getState(){
//        return state;
//    }
//
//    public ServerIdentifier getLeader(){
//        return this.leader;
//    }
//
//}
