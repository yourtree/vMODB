package dk.ku.di.dms.vms.coordinator.server.coordinator.options;

/**
 * Initialized with default values
 */
public class CoordinatorOptions {

    // a slack must be considered due to network overhead
    // e.g., by the time the timeout is reached, the time
    // taken to build the payload + sending the request over
    // the network may force the followers to initiate an
    // election. the slack is a conservative way to avoid
    // this from occurring, initiating a heartbeat sending
    // before the timeout is reached
    private int heartbeatSlack = 1000;

    // the batch window
    private long batchWindow = 60000; // a minute

    // timeout to keep track when to send heartbeats to followers
    private long heartbeatTimeout = 20000;

    private long txProcessTimeout = 10000;

    private int scheduledTasksThreadPoolSize = 1;

    public CoordinatorOptions withScheduledTasksThreadPoolSize(int scheduledTasksThreadPoolSize){
        this.scheduledTasksThreadPoolSize = scheduledTasksThreadPoolSize;
        return this;
    }

    public int getScheduledTasksThreadPoolSize() {
        return scheduledTasksThreadPoolSize;
    }

    public long getTxProcessTimeout() {
        return txProcessTimeout;
    }

    public CoordinatorOptions withTxProcessTimeout(long txProcessTimeout) {
        this.txProcessTimeout = txProcessTimeout;
        return this;
    }

    // default, no waiting for previous batch. many reasons, network congestion, node crashes, etc
    private BatchEmissionPolicy batchEmissionPolicy = BatchEmissionPolicy.OPTIMISTIC;

    public CoordinatorOptions withBatchEmissionPolicy(BatchEmissionPolicy batchEmissionPolicy){
        this.batchEmissionPolicy = batchEmissionPolicy;
        return this;
    }

    public BatchEmissionPolicy getBatchEmissionPolicy() {
        return batchEmissionPolicy;
    }

    public int getHeartbeatSlack() {
        return heartbeatSlack;
    }

    public CoordinatorOptions withHeartbeatSlack(int heartbeatSlack) {
        this.heartbeatSlack = heartbeatSlack;
        return this;
    }

    public long getBatchWindow() {
        return batchWindow;
    }

    public CoordinatorOptions withBatchWindow(long batchWindow) {
        this.batchWindow = batchWindow;
        return this;
    }

    public long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public CoordinatorOptions withHeartbeatTimeout(long heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
        return this;
    }
}
