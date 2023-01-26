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

    // when to get new transaction input events from http handler
    private long readTransactionInputTimeout = 10000;

    // thread pool to execute tasks, e.g., batch replication to replicas
    private int taskThreadPoolSize = 2;

    /**
     * thread pool for handling network events.
     * default is number of cores divided by 2
     */
    private int groupThreadPoolSize = Runtime.getRuntime().availableProcessors() / 2;

    // defines how the batch metadata is replicated across servers
    private BatchReplicationStrategy batchReplicationStrategy = BatchReplicationStrategy.NONE;

    public int getGroupThreadPoolSize() {
        return this.groupThreadPoolSize;
    }

    public void withGroupThreadPoolSize(int groupThreadPoolSize) {
        this.groupThreadPoolSize = groupThreadPoolSize;
    }

    public BatchReplicationStrategy getBatchReplicationStrategy() {
        return this.batchReplicationStrategy;
    }

    public CoordinatorOptions withBatchReplicationStrategy(BatchReplicationStrategy replicationStrategy){
        this.batchReplicationStrategy = replicationStrategy;
        return this;
    }

    public CoordinatorOptions withTaskThreadPoolSize(int scheduledTasksThreadPoolSize){
        this.taskThreadPoolSize = scheduledTasksThreadPoolSize;
        return this;
    }

    public int getTaskThreadPoolSize() {
        return this.taskThreadPoolSize;
    }

    public long getReadTransactionInputTimeout() {
        return this.readTransactionInputTimeout;
    }

    public CoordinatorOptions withReadTransactionInputTimeout(long readTransactionInputTimeout) {
        this.readTransactionInputTimeout = readTransactionInputTimeout;
        return this;
    }

    // default, no waiting for previous batch. many reasons, network congestion, node crashes, etc
    private BatchEmissionPolicy batchEmissionPolicy = BatchEmissionPolicy.BLOCKING;

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
        return this.batchWindow;
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
