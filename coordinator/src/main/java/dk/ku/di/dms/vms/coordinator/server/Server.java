package dk.ku.di.dms.vms.coordinator.server;

import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class Server {

    private AsynchronousServerSocketChannel serverSocket;

    private AsynchronousChannelGroup group;

    // general tasks, like sending info to vmss and other servers
    private ExecutorService taskExecutor;

    // even though we can start with a known number of servers, their payload may have changed after a crash
    private Map<Integer, ServerIdentifier> servers;

    // the identification of this server
    private ServerIdentifier me;

    // can be == me
    private AtomicReference<ServerIdentifier> leader;

    // to encapsulate operations in the memory-mapped file
    // private MetadataAPI metadataAPI;

    // private Metadata metadata;

    private final Queue<Byte> queue;

    // https://stackoverflow.com/questions/409932/java-timer-vs-executorservice
    // private ScheduledExecutorService scheduledBatchExecutor = Executors.newSingleThreadScheduledExecutor();

    // the heartbeat sending from the coordinator
    // private ScheduledExecutorService scheduledLeaderElectionExecutor = Executors.newSingleThreadScheduledExecutor();

    public Server() {
        this.queue = new ArrayBlockingQueue<>(1);
    }



    private static class BatchHandler implements Runnable {

        // constructor: all metadata

        @Override
        public void run() {

            // group
//            try {
//                AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool( taskExecutor );
//            } catch (IOException ignored) {}

            // close offset

            // get all vms that participated in the last batch

            // send commit info

            // wait for all acks given a timestamp

//            ScheduledFuture<?> schedulerHandler = scheduledBatchExecutor.schedule(new BatchHandler(), 30, TimeUnit.SECONDS );

        }

    }

}
