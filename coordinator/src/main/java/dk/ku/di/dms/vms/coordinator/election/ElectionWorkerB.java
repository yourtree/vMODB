package dk.ku.di.dms.vms.coordinator.election;

import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.election.schema.VoteRequest;
import dk.ku.di.dms.vms.coordinator.election.schema.VoteResponse;
import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static dk.ku.di.dms.vms.web_common.runnable.Constants.FINISHED;
import static dk.ku.di.dms.vms.web_common.runnable.Constants.NO_RESULT;
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
 */
public class ElectionWorkerB extends StoppableRunnable {

    private final AtomicInteger state;
    private static final int NEW          = 0;
    private static final int CANDIDATE    = 1; // running the protocol
    private static final int LEADER       = 2; // has received the ACKs from a majority
    private static final int FOLLOWER     = 3;

    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    // general tasks, like sending info to VMSs and other servers
    private final ExecutorService taskExecutor;

    // even though we can start with a known number of servers, their payload may have changed after a crash
    private final Map<Integer, ServerIdentifier> servers;

    // the identification of this server
    private final ServerIdentifier me;

    // can be == me
     private final AtomicReference<ServerIdentifier> leader;

    // a bounded time in which a leader election must occur, otherwise it should restart. in milliseconds
    // leaderElectionTimeWindow
    // safety, liveness????
    private final AtomicLong timeout;

    // queue serves as a channel to respond the calling thread
    private final Queue<Byte> signal;

    public ElectionWorkerB(AsynchronousServerSocketChannel serverSocket,
                           AsynchronousChannelGroup group,
                           ExecutorService taskExecutor,
                           ServerIdentifier me,
                           AtomicReference<ServerIdentifier> leader,
                           Map<Integer, ServerIdentifier> servers,
                           Queue<Byte> signal){
        this.state = new AtomicInteger(NEW);
        this.serverSocket = serverSocket;
        this.group = group;
        this.taskExecutor = taskExecutor;
        this.me = me;
        this.leader = leader;
        this.servers = servers;
        this.signal = signal;
        this.timeout = new AtomicLong(60000); // 1 minute
    }

    public ElectionWorkerB(AsynchronousServerSocketChannel serverSocket,
                           AsynchronousChannelGroup group,
                           ExecutorService taskExecutor,
                           ServerIdentifier me,
                           AtomicReference<ServerIdentifier> leader,
                           Map<Integer, ServerIdentifier> servers,
                           Queue<Byte> signal,
                           long timeout){
        this.state = new AtomicInteger(NEW);
        this.serverSocket = serverSocket;
        this.group = group;
        this.taskExecutor = taskExecutor;
        this.me = me;
        this.leader = leader;
        this.servers = servers;
        this.signal = signal;
        this.timeout = new AtomicLong(timeout);
    }

    private static class MessageCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {


        @Override
        public void completed(AsynchronousSocketChannel channel, Void attachment) {

            try {
                channel.setOption(TCP_NODELAY, true); // true disable the nagle's algorithm. not useful to have coalescence of messages in election'
                channel.setOption(SO_KEEPALIVE, false); // no need to keep alive now

                ByteBuffer readBuffer = ByteBuffer.allocate(128);

                channel.read(readBuffer).get();
            } catch(Exception e){

            }

        }

        @Override
        public void failed(Throwable exc, Void attachment) {

        }
    }

    /**
     * Thread responsible for handling messages related to leader election process.
     * I cannot set up the timeout here because I may spawn more than a thread to
     * handle messages. The timeout must be external to this thread, it is, the
     * thread that "owns" this one.
     *
     * Single thread, so no need to deal with data races.
     */
    private static class MessageHandler extends StoppableRunnable {

        private final ExecutorService executorService;

        private final AsynchronousServerSocketChannel serverSocket;

        private final AsynchronousChannelGroup group;

        private final Map<Integer, ServerIdentifier> servers;

        private final ServerIdentifier me;

        private final AtomicReference<ServerIdentifier> leader;

        private final Map<Integer, VoteResponse.VoteResponsePayload> responses;

        private final AtomicInteger state;

        private boolean voted;

        // number of servers
        private final int N;

        private final AtomicLong timeout;

        public MessageHandler(ExecutorService executorService,
                              AsynchronousServerSocketChannel serverSocket,
                              AsynchronousChannelGroup group,
                              ServerIdentifier me,
                              AtomicReference<ServerIdentifier> leader,
                              Map<Integer, ServerIdentifier> servers,
                              AtomicInteger state,
                              int N,
                              AtomicLong timeout) {
            this.executorService = executorService;
            this.serverSocket = serverSocket;
            this.group = group;
            this.me = me;
            this.servers = servers;
            this.leader = leader;
            this.responses = new HashMap<>(N);
            this.state = state;
            this.voted = false;
            this.N = N;
            this.timeout = timeout;
        }

        @Override
        public void run() {

            AsynchronousSocketChannel channel;

            logger.info("Initializing message handler. I am "+me.host+":"+me.port);

            while(!isStopped()){

                try {

                    channel = serverSocket.accept().get();
                    channel.setOption( TCP_NODELAY, true ); // true disable the nagle's algorithm. not useful to have coalescence of messages in election'
                    channel.setOption( SO_KEEPALIVE, false ); // no need to keep alive now

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

                            // it is a yes? add to responses
                            if (payload.response && !responses.containsKey(serverId)) { // avoid duplicate vote

                                //timeout.addAndGet(10000); // add 10 seconds

                                responses.put(serverId, payload);

                                // do I have the majority of votes?
                                // !voted prevent two servers from winning the election... but does not prevent
                                if (!voted && responses.size() + 1 > (N / 2)) {
                                    logger.info("I am leader. I am " + me.host + ":" + me.port);
                                    leader.set(me);
                                    sendLeaderRequests(group);
                                    state.set(LEADER);
                                }

                            }
                        }
                        case VOTE_REQUEST -> {

                            logger.info("Vote request received. I am " + me.host + ":" + me.port);

                            ServerIdentifier requestVote = VoteRequest.read(readBuffer);

                            if (voted) {
                                executorService.submit(new WriteTask(VOTE_RESPONSE, me, requestVote, group, false));
                                logger.info("Vote not granted, already voted. I am " + me.host + ":" + me.port);
                            } else {

                                if (requestVote.lastOffset > me.lastOffset) {
                                    //timeout.addAndGet(10000); // add 10 seconds
                                    // grant vote
                                    executorService.submit(new WriteTask(VOTE_RESPONSE, me, requestVote, group, true)).get();
                                    voted = true;
                                    logger.info("Vote granted. I am " + me.host + ":" + me.port);
                                } else if (requestVote.lastOffset < me.lastOffset) {
                                    executorService.submit(new WriteTask(VOTE_RESPONSE, me, requestVote, group, false));
                                    logger.info("Vote not granted. I am " + me.host + ":" + me.port);
                                } else { // equal

                                    if (requestVote.timestamp > me.timestamp) {
                                        //timeout.addAndGet(10000); // add 10 seconds
                                        // grant vote
                                        executorService.submit(new WriteTask(VOTE_RESPONSE, me, requestVote, group, true)).get();
                                        voted = true;
                                        logger.info("Vote granted. I am " + me.host + ":" + me.port);
                                    } else {
                                        executorService.submit(new WriteTask(VOTE_RESPONSE, me, requestVote, group, false)).get();
                                        logger.info("Vote not granted. I am " + me.host + ":" + me.port);
                                    } // chance being equal is minimal

                                }

                            }
                        }
                        case LEADER_REQUEST -> {

                            logger.info("Leader request received. I am " + me.host + ":" + me.port);

                            LeaderRequest.LeaderRequestPayload leaderRequest = LeaderRequest.read(readBuffer);

                            leader.set(servers.get(leaderRequest.hashCode()));

                            this.state.set(FOLLOWER);
                        }
                    }

                    channel.close();

                } catch (InterruptedException | ExecutionException | IOException ignored) {
                    logger.info("Error on reading message....");
                }

            }

            logger.info("Message handler is finished. I am "+me.host+":"+me.port);

        }

        private void sendLeaderRequests(AsynchronousChannelGroup group){
            logger.info("Sending leader requests. I am "+ me.host+":"+me.port);
            for(ServerIdentifier server : servers.values()){
                executorService.submit( new WriteTask( LEADER_REQUEST, me, server, group ) );
            }
        }

    }

    private static class WriteTask implements Callable<Boolean> {

        private final Logger logger = Logger.getLogger(this.getClass().getName());

        private final byte messageType;
        private final ServerIdentifier me;
        private final ServerIdentifier connectTo;
        private final AsynchronousChannelGroup group;

        private final Object[] args;

        public WriteTask(byte messageType, ServerIdentifier me, ServerIdentifier connectTo, AsynchronousChannelGroup group, Object... args){
            this.messageType = messageType;
            this.me = me;
            this.connectTo = connectTo;
            this.group = group;
            this.args = args;
        }

        @Override
        public Boolean call() {

            ByteBuffer buffer = ByteBuffer.allocate(128);

            try {

                InetSocketAddress address = new InetSocketAddress(connectTo.host, connectTo.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                channel.setOption( TCP_NODELAY, true ); // true disable the nagle's algorithm. not useful to have coalescence of messages in election
                channel.setOption( SO_KEEPALIVE, false ); // no need to keep alive now

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



    }

    /**
     * Should include while !isStopped....
     */
    @Override
    public void run() {

        logger.info("Initializing election round. I am "+me.host+":"+me.port);

        this.state.set(CANDIDATE);

        // works as a tiebreaker. updated on each round. yes, some servers may always be higher than others
        // me.timestamp = System.currentTimeMillis() + new Random().nextInt(10);
        me.timestamp = new Random().nextLong(300);

        // being single thread makes it easier to avoid data races
        MessageHandler messageHandler = new MessageHandler( this.taskExecutor, this.serverSocket, this.group, this.me, this.leader, servers, this.state, servers.size(), timeout );
        taskExecutor.submit( messageHandler );

        //Map<ServerIdentifier, Future<Boolean>> mapOfFutureResponses =
        sendVoteRequests(group);

        int state_ = state.get();
        long timeout_ = timeout.get();

        /* while majority cannot be established, we cannot proceed safely */
        long elapsed = 0L;
        long startTime = System.currentTimeMillis();
        while(elapsed < timeout_ && state_ == CANDIDATE){
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {} // this can make messages not being handled. it is the timeout making this happen
            elapsed = System.currentTimeMillis() - startTime;
            state_ = state.get();
            timeout_ = timeout.get();
        }

        logger.info("Event loop has finished. I am "+me.host+":"+me.port+" and my state is "+state_);

        // now check whether I have enough votes or whether I should send a response vote to some server
        if(state_ == LEADER || state_ == FOLLOWER){
            signal.add(FINISHED);
        } else {
            // nothing has been defined... we should try another round
            signal.add(NO_RESULT);
        }

        messageHandler.stop();

    }

    private void sendVoteRequests(AsynchronousChannelGroup group) {
        logger.info("Sending vote requests. I am "+ me.host+":"+me.port);
        for(ServerIdentifier server : servers.values()){
             taskExecutor.submit( new WriteTask( VOTE_REQUEST, me, server, group ) );
        }
    }

}
