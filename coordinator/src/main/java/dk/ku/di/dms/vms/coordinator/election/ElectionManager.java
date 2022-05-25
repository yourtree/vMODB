package dk.ku.di.dms.vms.coordinator.election;

import dk.ku.di.dms.vms.coordinator.election.schema.*;
import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * An election task is a thread that encapsulates all subtasks (i.e., threads)
 * necessary to complete a leader election. Only after this thread finishes,
 * a server can act as a leader or follower.
 *
 * define leader based on highest offset and timestamp
 * while has not received all votes and election timeout has not timed out, continue
 *
 * We assume the nodes are fixed. Later we revisit this choice.
 * TODO Cluster membership management (e.g., adding nodes, removing nodes, replacing nodes)
 */
public class ElectionManager extends StoppableRunnable {

    private final AtomicInteger state;
    private static final int NEW          = 0;
    private static final int CANDIDATE    = 1; // running the protocol
    private static final int LEADER       = 2; // has received the ACKs from a majority
    private static final int FOLLOWER     = 3;

    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    // general tasks, like sending info to vmss and other servers
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
    private final long timeout = 10000; // 10 seconds

    // queue serves as a channel to respond the calling thread
    private final Queue<Byte> signal;

    public ElectionManager(AsynchronousServerSocketChannel serverSocket,
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
    }

    /**
     * Thread responsible for handling messages related to leader election process.
     * I cannot set up the timeout here because I may spawn more than a thread to
     * handle messages. The timeout must be external to this thread, it is, the
     * thread that "owns" this one.
     */
    private static class ElectionMessageHandler extends StoppableRunnable {

        private final AsynchronousServerSocketChannel serverSocket;

        private final Map<Integer, ServerIdentifier> servers;

        private final ServerIdentifier me;

        private final AtomicReference<ServerIdentifier> leader;

        private final ByteBuffer readBuffer;

        private final ByteBuffer writeBuffer;

        private final Map<Integer, VoteResponse.VoteResponsePayload> responses;

        private final AtomicInteger state;

        private boolean voted;

        // number of servers
        private final int N;

        public ElectionMessageHandler(AsynchronousServerSocketChannel serverSocket,
                                      ServerIdentifier me,
                                      AtomicReference<ServerIdentifier> leader,
                                      Map<Integer, ServerIdentifier> servers,
                                      AtomicInteger state,
                                      int N) {
            this.serverSocket = serverSocket;
            this.me = me;
            this.servers = servers;
            this.leader = leader;
            this.responses = new HashMap<>(N);
            this.state = state;
            this.voted = false;
            this.N = N;

            // FIXME -- maximum size of a leader election message....
            this.readBuffer = ByteBuffer.allocateDirect(128);
            this.writeBuffer = ByteBuffer.allocateDirect(128);
        }

        @Override
        public void run() {

            AsynchronousSocketChannel channel;

            while(!isStopped()){

                logger.info("Initializing message handler. I am "+me.host+":"+me.port);

                try {

                    channel = serverSocket.accept().get();

                    channel.setOption( TCP_NODELAY, true ); // true disable the nagle's algorithm. not useful to have coalescence of messages in election'
                    channel.setOption( SO_KEEPALIVE, false ); // no need to keep alive now

                    channel.read(readBuffer).get();

                    byte messageIdentifier = readBuffer.get();

                    // message identifier
                    readBuffer.position(0);

                    switch(messageIdentifier){

                        case VOTE_RESPONSE : {

                            logger.info("Vote response received. I am "+me.host+":"+me.port);

                            VoteResponse.VoteResponsePayload payload = VoteResponse.read(readBuffer);
                            int serverId = Objects.hash( payload.host, payload.port );

                            // it is a yes? add to responses
                            if( !responses.containsKey( serverId ) && payload.response ) {
                                responses.put( serverId, payload );
                            }

                            // do I have the majority of votes?
                            if (responses.size() + 1 > (N /2)){
                                state.set(LEADER);
                                leader.set( me );
                            }

                            readBuffer.flip();

                            break;
                        }

                        case VOTE_REQUEST : {

                            logger.info("Vote request received. I am "+me.host+":"+me.port);

                            if(voted){
                                VoteResponse.write(writeBuffer, me, false);
                            } else {

                                ServerIdentifier requestVote = VoteRequest.read(readBuffer);

                                if (requestVote.lastOffset > me.lastOffset) {
                                    VoteResponse.write(writeBuffer, me, true);
                                    voted = true;
                                } else if (requestVote.lastOffset < me.lastOffset) {
                                    VoteResponse.write(writeBuffer, me, false);
                                } else { // equal

                                    if (requestVote.timestamp > me.timestamp) {
                                        VoteResponse.write(writeBuffer, me, true);
                                        voted = true;
                                    } else {
                                        VoteResponse.write(writeBuffer, me, false);
                                    }

                                }

                                // even though I voted, cannot go to follower yet

                                readBuffer.flip();

                            }

                            channel.write(writeBuffer).get();
                            writeBuffer.flip();

                            break;
                        }


                        case LEADER_REQUEST : {

                            logger.info("Leader request received. I am "+me.host+":"+me.port);

                            LeaderRequest.LeaderRequestPayload leaderRequest = LeaderRequest.read(readBuffer);

                            leader.set( servers.get(leaderRequest.hashCode()) );

                            this.state.set( FOLLOWER );

                            //this.stop();
                            break;
                        }

                    }

                    // have we received all responses? if yes, check if we have majority, if so we are leader and then send message to all stating we are leader
                    // this step some servers may also have failed... so they need to enter as followers and so on...

                    channel.close();

                } catch (InterruptedException | ExecutionException | IOException ignored) {}

            }

        }

    }

    private static class WriteTask implements Callable<Boolean> {

        private final Logger logger = Logger.getLogger(this.getClass().getName());

        private final byte messageType;
        private final ServerIdentifier server; // me or the leader, depending on the call
        private final ServerIdentifier other;
        private final AsynchronousChannelGroup group;

        public WriteTask(byte messageType, ServerIdentifier server, ServerIdentifier other, AsynchronousChannelGroup group){
            this.messageType = messageType;
            this.server = server;
            this.other = other;
            this.group = group;
        }

        @Override
        public Boolean call() {

            ByteBuffer buffer = ByteBuffer.allocateDirect(128);

            try {

                InetSocketAddress address = new InetSocketAddress(other.host, other.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                channel.setOption( TCP_NODELAY, true ); // true disable the nagle's algorithm. not useful to have coalescence of messages in election'
                channel.setOption( SO_KEEPALIVE, false ); // no need to keep alive now

                try {
                    channel.connect(address).get();
                } catch (InterruptedException | ExecutionException e) {
                    // cannot connect to host
                    return false;
                }

                // now send the request
                if(messageType == VOTE_REQUEST) {
                    VoteRequest.write(buffer, server);
                } else if( messageType == LEADER_REQUEST ){
                    LeaderRequest.write(buffer, server);
                }

//                else if ( messageType == LEADER_INFO ){
//                    LeaderInfo.write(buffer, server);
//                } else if (messageType == LEADER_RESPONSE ){
//                    LeaderResponse.write(buffer, server);
//                }

                Integer write = channel.write(buffer).get();

                // number of bytes written
                if (write == -1) {
                    return false;
                }

            } catch(Exception ignored){ return false; }

            return true;

        }

    }

    @Override
    public void run() {

        logger.info("Initializing election round. I am "+me.host+":"+me.port);

        this.state.set(CANDIDATE);

        // works as a term. updated on each round. yes, some servers may always be higher than others
        me.timestamp = System.currentTimeMillis();

        // TODO define how many. One is enough? Another design is making this a completion handler is initiating the reads on the loop below...
        taskExecutor.submit( new ElectionMessageHandler(
                this.serverSocket, this.me, this.leader, servers, this.state, servers.size() ) );

        logger.info("Event listener set. I am "+me.host+":"+me.port);

        //Map<ServerIdentifier, Future<Boolean>> mapOfFutureResponses =
                sendVoteRequests(group);

        int state_ = state.get();

        /* while majority cannot be established, we cannot proceed safely */
        long elapsed = 0L;
        long startTime = System.currentTimeMillis();
        while(elapsed < timeout && state_ == CANDIDATE){
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            elapsed = System.currentTimeMillis() - startTime;
            state_ = state.get();
        }

        logger.info("Event loop has finished. I am "+me.host+":"+me.port);

        // now check whether I have enough votes or whether I should send a response vote to some server
        if(state_ == LEADER){
            // leader = me;
            sendLeaderRequests(group);
            signal.add(Constants.FINISHED);
        } else if (state_ == FOLLOWER){
            // sit idle
            signal.add(Constants.FINISHED);
        } else {
            // nothing has been defined... we should try another round
            signal.add(Constants.NO_RESULT);
        }

        logger.info("Event loop has finished. I am "+me.host+":"+me.port+" and my state is "+state_);

    }

//    private Map<ServerIdentifier, Future<Boolean>> sendVoteRequests(AsynchronousChannelGroup group) {
//        Map<ServerIdentifier, Future<Boolean>> mapOfFutureResponses = new HashMap<>();
//        for(ServerIdentifier server : servers){
//            mapOfFutureResponses.put(server, taskExecutor.submit( new WriteTask( VOTE_REQUEST, me, server, group ) ) );
//        }
//        return mapOfFutureResponses;
//    }

    private void sendVoteRequests(AsynchronousChannelGroup group) {
        for(ServerIdentifier server : servers.values()){
             taskExecutor.submit( new WriteTask( VOTE_REQUEST, me, server, group ) );
        }
    }

    private void sendLeaderRequests(AsynchronousChannelGroup group){
        for(ServerIdentifier server : servers.values()){
            taskExecutor.submit( new WriteTask( LEADER_REQUEST, me, server, group ) );
        }
    }

}
