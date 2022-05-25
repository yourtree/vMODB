package dk.ku.di.dms.vms.coordinator.server;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Server {

    // to encapsulate operations in the memory-mapped file
    // private MetadataAPI metadataAPI;

    // private Metadata metadata;

    private final Queue<Byte> queue;

    // https://stackoverflow.com/questions/409932/java-timer-vs-executorservice
    // private ScheduledExecutorService scheduledBatchExecutor = Executors.newSingleThreadScheduledExecutor();

    // the heartbeat sending from the coordinator
    // private ScheduledExecutorService scheduledLeaderElectionExecutor = Executors.newSingleThreadScheduledExecutor();

    public Server() {
        this.queue = new LinkedBlockingDeque<>(1);
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
