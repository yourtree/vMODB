package dk.ku.di.dms.vms.coordinator.server.follower;

import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;
import dk.ku.di.dms.vms.coordinator.server.infra.BatchContext;
import dk.ku.di.dms.vms.coordinator.server.infra.BufferManager;
import dk.ku.di.dms.vms.coordinator.server.infra.ConnectionMetadata;
import dk.ku.di.dms.vms.coordinator.server.schema.internal.Presentation;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * Follower
 * Initial design: sit idle and wait for new election in case heartbeat does not arrive on time
 */
public final class Follower extends SignalingStoppableRunnable {

    // TODO finish when a new leader is elected needs to send a batch abort request. but this is in the coordinator class...

    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    // general tasks, like sending info to VMSs and other servers
    private final ExecutorService taskExecutor;

    // required in order to send votes if new election process starts
    private final Map<Integer, ServerIdentifier> servers;

    // private final ConnectionMetadata serverConnectionMetadata;

    private final ServerIdentifier me;

    private final ServerIdentifier leader;

    private final Map<Long, BatchContext> batchContextMap;

    private volatile long lastBatchOffsetCommitted;

    // refers to last committed batch
    private Map<String, Long> lastTidOfBatchPerVms;

    private int maximumLeaderConnectionAttempt;

    public Follower(AsynchronousServerSocketChannel serverSocket, AsynchronousChannelGroup group, ExecutorService taskExecutor, Map<Integer, ServerIdentifier> servers, ServerIdentifier me, ServerIdentifier leader) {

        // network and executor
        this.serverSocket = Objects.requireNonNull(serverSocket);
        this.group = group;
        this.taskExecutor = Objects.requireNonNull(taskExecutor);

        this.servers = servers;
        this.me = me;
        this.leader = leader;
        this.batchContextMap = new HashMap<>();
    }

    // accept handler

    // read completion handler

    // handle batch replication

    @Override
    public void run() {

        // connect to leader
        connectToLeader();

        // setup accept handler, since new servers may enter the system. besides

        while(!isStopped()) {

            // if heartbeat timed out,leave loop


            // should try three times connection to leader, otherwise starts a new election...
        }

    }

    private void connectToLeader(){

        try {

            InetSocketAddress address = new InetSocketAddress(leader.host, leader.port);
            AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
            channel.setOption(TCP_NODELAY, true);
            channel.setOption(SO_KEEPALIVE, true);

            channel.connect(address).get();

            ConnectionMetadata connectionMetadata = new ConnectionMetadata(
                    leader.hashCode(),
                    ConnectionMetadata.NodeType.SERVER,
                    BufferManager.loanByteBuffer(),
                    BufferManager.loanByteBuffer(),
                    channel,
                    // new ReentrantLock()
                    null // no need to lock, only one thread writing
            );

            Presentation.write( connectionMetadata.writeBuffer, me );

            channel.write( connectionMetadata.writeBuffer ).get();

            connectionMetadata.writeBuffer.clear();

            channel.read( connectionMetadata.readBuffer, connectionMetadata, new ReadCompletionHandler() );

            // set up read completion handler for receiving heartbeats
            // perhaps good to introduce a delta (to reflect possible network latency introduced by the link)

            // if heartbeat threshold has been achieved, finish this class and return (signaling... false?)
        } catch (Exception ignored) {
            logger.info("Error connecting to host. I am " + me.host + ":" + me.port + " and the target is " + leader.host + ":" + leader.port);

        }

    }

    private class ReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata attachment) {

        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata attachment) {

        }

    }

}
