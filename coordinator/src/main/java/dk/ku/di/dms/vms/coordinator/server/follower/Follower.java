package dk.ku.di.dms.vms.coordinator.server.follower;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import dk.ku.di.dms.vms.web_common.meta.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.buffer.BufferManager;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchReplication;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchReplicationAck;
import dk.ku.di.dms.vms.web_common.meta.schema.control.Presentation;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static dk.ku.di.dms.vms.web_common.meta.Constants.BATCH_REPLICATION;
import static dk.ku.di.dms.vms.web_common.meta.Constants.HEARTBEAT;
import static java.lang.Thread.sleep;
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

    // are we using this?
    // private final Map<Long, BatchContext> batchContextMap;

    private volatile long lastBatchOffsetCommitted;

    // refers to last committed batch
    private Map<String, Long> lastTidOfBatchPerVms;

    private volatile long lastTimestamp;

    private final FollowerOptions options;

    private final Gson gson;

    public Follower(AsynchronousServerSocketChannel serverSocket,
                    AsynchronousChannelGroup group,
                    ExecutorService taskExecutor,
                    FollowerOptions options,
                    Map<Integer, ServerIdentifier> servers,
                    ServerIdentifier me,
                    ServerIdentifier leader,
                    Gson gson) {

        // network and executor
        this.serverSocket = Objects.requireNonNull(serverSocket);
        this.group = group;
        this.taskExecutor = Objects.requireNonNull(taskExecutor);

        // options
        this.options = options;

        this.servers = servers;
        this.me = me;
        this.leader = leader;

        // batch
        // this.batchContextMap = new HashMap<>();

        this.gson = gson;
    }

    @Override
    public void run() {

        // accept handler
        serverSocket.accept( null, new AcceptCompletionHandler());

        // connect to leader
        if(!connectToLeader()) {
            this.signal.add( NO_RESULT );
            return;
        }

        // start timestamp
        lastTimestamp = System.nanoTime();

        // setup accept handler, since new servers may enter the system. besides
        long timeout = options.getHeartbeatTimeout();

        while(!isStopped()) {

            // if heartbeat timed out, leave loop
            // can just sleep until the next timestamp (slightly after is better due to network latency)
            try {
                sleep(timeout);
            } catch (InterruptedException ignored) { }

            if (System.nanoTime() - lastTimestamp >= timeout){
                stop();
                this.signal.add( NO_RESULT );
            }

        }

    }

    private boolean connectToLeader(){

        ByteBuffer readBuffer = BufferManager.loanByteBuffer();
        ByteBuffer writeBuffer = BufferManager.loanByteBuffer();

        // should try three times connection to leader, otherwise starts a new election...
        int maxAttempts = options.getMaximumLeaderConnectionAttempt();
        boolean finished = false;

        while(!finished && maxAttempts > 0) {

            try {

                InetSocketAddress address = new InetSocketAddress(leader.host, leader.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                channel.connect(address).get();

                ConnectionMetadata connectionMetadata = new ConnectionMetadata(
                        leader.hashCode(),
                        ConnectionMetadata.NodeType.SERVER,
                        readBuffer,
                        writeBuffer,
                        channel,
                        // new ReentrantLock()
                        null // no need to lock, only one thread writing
                );

                Presentation.writeServer(connectionMetadata.writeBuffer, me);

                channel.write(connectionMetadata.writeBuffer).get();

                connectionMetadata.writeBuffer.clear();

                channel.read(connectionMetadata.readBuffer, connectionMetadata, new ReadCompletionHandler());

                // set up read completion handler for receiving heartbeats
                // perhaps good to introduce a delta (to reflect possible network latency introduced by the link)

                // if heartbeat threshold has been achieved, finish this class and return (signaling... false?)

                finished = true;

            } catch (Exception ignored) {
                logger.info("Error connecting to host. I am " + me.host + ":" + me.port + " and the target is " + leader.host + ":" + leader.port);
            }

            if(!finished) maxAttempts--;

        }

        return finished;

    }

    private class ReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            ByteBuffer readBuffer = connectionMetadata.readBuffer;

            byte type = readBuffer.get();

            if(type == HEARTBEAT){

                // this is the only thread updating it
                lastTimestamp = System.nanoTime();

                readBuffer.clear();

                connectionMetadata.channel.read( readBuffer, connectionMetadata, this );


            } else if(type == BATCH_REPLICATION){

                BatchReplication.BatchReplicationPayload payload = BatchReplication.read( readBuffer );

                readBuffer.clear();

                try {

                    try {

                        lastTidOfBatchPerVms = gson.fromJson(payload.vmsTidMap(), new TypeToken<Map<String, Long>>() {}.getType());
                        // actually this is not yet sure... only after receiving the next one... but let's consider this for now
                        lastBatchOffsetCommitted = payload.batch();

                        // confirming
                        BatchReplicationAck.write( connectionMetadata.writeBuffer, payload.batch() );

                    } catch (JsonSyntaxException e) { // error in the json
                        BatchReplicationAck.write(connectionMetadata.writeBuffer, 0);
                    }
                    connectionMetadata.channel.write( connectionMetadata.writeBuffer ).get();
                    connectionMetadata.writeBuffer.clear();

                } catch (ExecutionException | InterruptedException ignored) { }
                finally {
                    if(connectionMetadata.channel != null && connectionMetadata.channel.isOpen()) {
                        try {
                            connectionMetadata.channel.close();
                        } catch (IOException ignored) {}
                    }
                    BufferManager.returnByteBuffer(connectionMetadata.writeBuffer);
                    BufferManager.returnByteBuffer(connectionMetadata.readBuffer);
                }

            }

        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            AsynchronousSocketChannel channel = connectionMetadata.channel;
            if(channel != null && channel.isOpen()) {
                try {
                    if(!channel.getOption( SO_KEEPALIVE )) {
                        channel.close();
                        return;
                    }
                } catch (IOException ignored) {}
            }
            connectionMetadata.readBuffer.clear();
            // read again
            connectionMetadata.channel.read( connectionMetadata.readBuffer, connectionMetadata, this );
        }

    }

    /**
     * Who would like to connect?
     * The coordinator for batch replication. This is the only assumption for now.
     * If that changes, we must read the presentation header and add the node to the servers list.
     */
    private class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {

            ByteBuffer readBuffer = BufferManager.loanByteBuffer();
            ByteBuffer writeBuffer = BufferManager.loanByteBuffer();

            // if it is a VMS, need to forward to the leader ? better to let the vms know
            try{
                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, false);

                ConnectionMetadata connectionMetadata = new ConnectionMetadata(
                        leader.hashCode(),
                        ConnectionMetadata.NodeType.SERVER,
                        readBuffer,
                        writeBuffer,
                        channel,
                        null // no need to lock, only one thread writing
                );

                channel.read( readBuffer, connectionMetadata, new ReadCompletionHandler() );

            } catch(Exception e){
                if(channel != null && !channel.isOpen()){
                    BufferManager.returnByteBuffer(readBuffer);
                    BufferManager.returnByteBuffer(writeBuffer);
                }
            }

            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            }

        }

        @Override
        public void failed(Throwable exc, Void void_) {
            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            }
        }

    }

}
