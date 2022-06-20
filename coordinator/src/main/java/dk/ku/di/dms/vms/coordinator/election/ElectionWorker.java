package dk.ku.di.dms.vms.coordinator.election;

import dk.ku.di.dms.vms.coordinator.election.schema.*;
import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static dk.ku.di.dms.vms.web_common.runnable.Constants.FINISHED;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * An election task is a thread that encapsulates all subtasks (i.e., threads)
 * necessary to complete a leader election. Only after this thread finishes,
 * a server can act as a leader or follower.
 *
 * Define leader based on highest offset and timestamp
 * while election is not defined and election timeout has not timed out, continue
 *
 * We assume the nodes are fixed. Later we revisit this choice.
 * TODO Cluster membership management (e.g., adding nodes, removing nodes, replacing nodes)
 * TODO make connections fixed, so can be subsequently reused by the leader/follower class
 */
public final class ElectionWorker extends SignalingStoppableRunnable {

    private volatile int state;
    public static final int NEW          = 0;
    public static final int CANDIDATE    = 1; // running the protocol
    public static final int LEADER       = 2; // has received the ACKs from a majority
    public static final int FOLLOWER     = 3;

    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    // general tasks, like sending info to VMSs and other servers
    private final ExecutorService taskExecutor;

    // even though we can start with a known number of servers, their payload may have changed after a crash
    private final Map<Integer, ServerIdentifier> servers;

    // the identification of this server
    private final ServerIdentifier me;

    // can be == me
    // only one thread modifying it, no need for atomic reference
    private volatile ServerIdentifier leader;

    // a bounded time in which a leader election must occur, otherwise it should restart. in milliseconds
    // only one thread modifying, no need to use AtomicLong
    private final AtomicLong timeout;

    // https://stackoverflow.com/questions/3786825/volatile-boolean-vs-atomicboolean
    private final AtomicBoolean voted;

    private final MessageHandler messageHandler;

    public ElectionWorker(AsynchronousServerSocketChannel serverSocket,
                          AsynchronousChannelGroup group,
                          ExecutorService taskExecutor,
                          ServerIdentifier me,
                          Map<Integer, ServerIdentifier> servers){
        super();
        this.state = NEW;
        this.serverSocket = serverSocket;
        this.group = group;
        this.taskExecutor = taskExecutor;
        this.me = me;
        this.servers = servers;
        this.timeout = new AtomicLong(60000 ); // 1 minute default
        this.state = CANDIDATE;
        this.voted = new AtomicBoolean(false);
        this.messageHandler = new MessageHandler();
    }

    public ElectionWorker(AsynchronousServerSocketChannel serverSocket,
                          AsynchronousChannelGroup group,
                          ExecutorService taskExecutor,
                          ServerIdentifier me,
                          Map<Integer, ServerIdentifier> servers,
                          long timeout){
        super();
        this.state = NEW;
        this.serverSocket = serverSocket;
        this.group = group;
        this.taskExecutor = taskExecutor;
        this.me = me;
        this.servers = servers;
        this.timeout = new AtomicLong( timeout );
        this.state = CANDIDATE;
        this.voted = new AtomicBoolean(false);
        this.messageHandler = new MessageHandler();
    }

    /**
     * Thread responsible for handling messages related to leader election process.
     * I cannot set up the timeout here because I may spawn more than a thread to
     * handle messages. The timeout must be external to this thread, it is, the
     * thread that "owns" this one.
     * https://blog.gceasy.io/2021/02/24/java-threads-may-not-be-memory-efficient/
     * Single thread, so no need to deal with data races.
     */
    private class MessageHandler extends StoppableRunnable {

        // positive responses
        private final Map<Integer, VoteResponse.VoteResponsePayload> responses;
        private final Object _lock = new Object();
        private final int N;

        public MessageHandler() {
            this.N = servers.size();
            this.responses = new ConcurrentHashMap<>(N);
        }

        public void resetVoteResponses(){
            synchronized (_lock) {
                this.responses.clear();
            }
        }

        @Override
        public void run() {

            AsynchronousSocketChannel channel;

            logger.info("Initializing message handler. I am "+me.host+":"+me.port);

            while(!isStopped()){

                try {

                    channel = serverSocket.accept().get();
                    channel.setOption( TCP_NODELAY, true ); // true disables the nagle's algorithm. not useful to have coalescence of messages in election
                    channel.setOption( SO_KEEPALIVE, false ); // no need to keep alive here

                    ByteBuffer readBuffer = ByteBuffer.allocate(128);

                    channel.read(readBuffer).get();

                    byte messageIdentifier = readBuffer.get(0);

                    // message identifier
                    readBuffer.position(1);

                    logger.info("Message read. I am "+me.host+":"+me.port+" identifier is "+messageIdentifier);

                    switch (messageIdentifier) {
                        case VOTE_RESPONSE -> {

                            VoteResponse.VoteResponsePayload payload = VoteResponse.read(readBuffer);

                            logger.info("Vote response received: "+ payload.response +". I am " + me.host + ":" + me.port);

                            int serverId = Objects.hash(payload.host, payload.port);

                            // is it a yes? add to responses
                            if (payload.response) {

                                synchronized (_lock) { // the responses are perhaps being reset
                                    if (!responses.containsKey(serverId)) { // avoid duplicate vote

                                        responses.put(serverId, payload);

                                        // do I have the majority of votes?
                                        // !voted prevent two servers from winning the election... but does not prevent
                                        if (!voted.get() && responses.size() + 1 > (N / 2)) {
                                            logger.info("I am leader. I am " + me.host + ":" + me.port);
                                            leader = me; // only this thread is writing
                                            sendLeaderRequests();
                                            state = LEADER;
                                        }

                                    }
                                }
                            }
                        }
                        case VOTE_REQUEST -> {

                            logger.info("Vote request received. I am " + me.host + ":" + me.port);

                            ServerIdentifier serverRequestingVote = VoteRequest.read(readBuffer);

                            if (voted.get()) {
                                taskExecutor.submit(new WriteTask(VOTE_RESPONSE, serverRequestingVote, false));
                                logger.info("Vote not granted, already voted. I am " + me.host + ":" + me.port);
                            } else {

                                // TO AVOID TWO (OR MORE) LEADERS!!!
                                // if a server is requesting a vote, it means this server is in another round, so I need to remove his vote from my (true) responses
                                // because this server may give a vote to another server
                                // in this case I will send a vote request again
                                boolean previousVoteReceived = false;
                                synchronized (_lock) { // the responses are perhaps being reset, cannot count on this vote anymore
                                    if (responses.get(serverRequestingVote.hashCode()) != null) {
                                        responses.remove( serverRequestingVote.hashCode() );
                                        previousVoteReceived = true;
                                    }
                                }

                                if (serverRequestingVote.lastOffset > me.lastOffset) {
                                    // grant vote
                                    taskExecutor.submit(new WriteTask(VOTE_RESPONSE, serverRequestingVote, true));
                                    voted.set(true);
                                    logger.info("Vote granted. I am " + me.host + ":" + me.port);
                                } else if (serverRequestingVote.lastOffset < me.lastOffset) {
                                    taskExecutor.submit(new WriteTask(VOTE_RESPONSE, me, serverRequestingVote, group, false));
                                    logger.info("Vote not granted. I am " + me.host + ":" + me.port);
                                } else { // equal

                                    if (serverRequestingVote.hashCode() > me.hashCode()) {
                                        // grant vote
                                        taskExecutor.submit(new WriteTask(VOTE_RESPONSE, serverRequestingVote, true));
                                        voted.set(true);
                                        logger.info("Vote granted. I am " + me.host + ":" + me.port);
                                    } else {
                                        taskExecutor.submit(new WriteTask(VOTE_RESPONSE, serverRequestingVote, false));
                                        logger.info("Vote not granted. I am " + me.host + ":" + me.port);
                                    }

                                }

                                // this is basically attempt to refresh the vote in case of intersecting distinct rounds in different servers
                                if(!voted.get() && previousVoteReceived){
                                    taskExecutor.submit( new WriteTask( VOTE_REQUEST, serverRequestingVote ) );
                                }

                            }
                        }
                        case LEADER_REQUEST -> {
                            logger.info("Leader request received. I am " + me.host + ":" + me.port);
                            LeaderRequest.LeaderRequestPayload leaderRequest = LeaderRequest.read(readBuffer);
                            leader = servers.get(leaderRequest.hashCode());
                            state = FOLLOWER;
                        }
                    }

                    channel.close();

                } catch (InterruptedException | ExecutionException | IOException ignored) {
                    logger.info("Error on reading message....");
                }

            }

            logger.info("Message handler is finished. I am "+me.host+":"+me.port);

        }

        private void sendLeaderRequests(){
            logger.info("Sending leader requests. I am "+ me.host+":"+me.port);
            for(ServerIdentifier server : servers.values()){
                taskExecutor.submit( new WriteTask( LEADER_REQUEST, server ) );
            }
        }

    }

    private class WriteTask implements Callable<Boolean> {

        private final byte messageType;
        private final ServerIdentifier connectTo;
        private final Object[] args;

        public WriteTask(byte messageType, ServerIdentifier connectTo, Object... args){
            this.messageType = messageType;
            this.connectTo = connectTo;
            this.args = args;
        }

        @Override
        public Boolean call() {

            ByteBuffer buffer = ByteBuffer.allocate(128);

            try {

                InetSocketAddress address = new InetSocketAddress(connectTo.host, connectTo.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                channel.setOption( TCP_NODELAY, true );
                channel.setOption( SO_KEEPALIVE, false );

                try {
                    channel.connect(address).get();
                } catch (InterruptedException | ExecutionException e) {
                    // cannot connect to host
                    logger.info("Error connecting to host. I am "+ me.host+":"+me.port+" and the target is "+ connectTo.host+":"+ connectTo.port);
                    return false;
                }

                // now send the request
                if(messageType == VOTE_REQUEST) {
                    VoteRequest.write(buffer, me);
                } else if( messageType == LEADER_REQUEST ){
                    LeaderRequest.write(buffer, me);
                } else if (messageType == VOTE_RESPONSE){
                    VoteResponse.write( buffer, me, (Boolean) args[0]);
                }

                /*
                 * https://www.baeldung.com/java-bytebuffer
                 *    Capacity: the maximum number of data elements the buffer can hold
                 *    Limit: an index to stop read or write
                 *    Position: the current index to read or write
                 *    Mark: a remembered position
                 */
                Integer write = channel.write(ByteBuffer.wrap( buffer.array() )).get();

                // number of bytes written
                if (write == -1) {
                    logger.info("Error on write (-1). I am "+ me.host+":"+me.port+" message type is "+messageType);
                    return false;
                }

                logger.info("Write performed. I am "+ me.host+":"+me.port+" message type is "+messageType+" and return was "+write);

                if (channel.isOpen()) {
                    channel.close();
                }

                return true;

            } catch(Exception ignored){
                logger.info("Error on write. I am "+ me.host+":"+me.port+" message type is "+messageType);
                return false;
            }

        }

    }

    private void runRound(){

        sendVoteRequests();

        int state_ = state;
        long timeout_ = timeout.get();

        /* while majority cannot be established, we cannot proceed safely */
        long elapsed = 0L;
        long startTime = System.currentTimeMillis();
        while(elapsed < timeout_ && state_ == CANDIDATE){
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {} // this can make messages not being handled. it is the timeout making this happen
            elapsed = System.currentTimeMillis() - startTime;
            state_ = state;
            timeout_ = timeout.get();
        }

        // round is over, responses received no longer hold
        // this is the same in all servers
        if(state_ == CANDIDATE){
            messageHandler.resetVoteResponses();// data races may still allow old votes to be computed
            voted.set(false); // make sure the next round this server is able to vote
        }

        logger.info("Event loop has finished. I am "+me.host+":"+me.port+" and my state is "+state_);

    }

    /**
     *
     */
    @Override
    public void run() {

        double roundDeltaIncrease = 0.25;

        logger.info("Initializing election round. I am "+me.host+":"+me.port);

        // being single thread makes it easier to avoid data races

        taskExecutor.submit( messageHandler );

        while(!isStopped() && state == CANDIDATE){

            // the round is an abstraction to avoid a vote given from holding forever (e.g., the requesting node may have crashed)
            runRound();

            // define a new delta since defining a leader is taking too long
            long timeout_ = timeout.get();
            timeout_ = (long) (timeout_ + (timeout_ * roundDeltaIncrease));
            timeout.set(timeout_); // increment since nothing has been defined

        }

        // stop server
        messageHandler.stop();

        // signal the server
        signal.add(FINISHED);

    }

    private void sendVoteRequests() {
        logger.info("Sending vote requests. I am "+ me.host+":"+me.port);
        for(ServerIdentifier server : servers.values()){
             taskExecutor.submit( new WriteTask( VOTE_REQUEST, server ) );
        }
    }

    public int getState(){
        return state;
    }

    public ServerIdentifier getLeader(){
        return this.leader;
    }

}
