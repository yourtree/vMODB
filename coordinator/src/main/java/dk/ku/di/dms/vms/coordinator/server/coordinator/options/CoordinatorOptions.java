package dk.ku.di.dms.vms.coordinator.server.coordinator.options;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;

/**
 * Rules that govern how a coordinator must behave.
 * Values initialized with default parameters.
 */
public final class CoordinatorOptions {

    private static final int NUM_CPUS = Runtime.getRuntime().availableProcessors();

    // the batch window. a minute by default
    private long batchWindow = 60000;

    // thread pool to execute tasks, e.g., batch replication to replicas
    private int taskThreadPoolSize = NUM_CPUS / 2;

    private int networkBufferSize = MemoryUtils.DEFAULT_PAGE_SIZE;

    private int osBufferSize = 0;

    private int networkSendTimeout = 1000;

    /**
     * thread pool for handling network events.
     * default is number of cores divided by 2
     */
    private int groupThreadPoolSize = NUM_CPUS / 2;

    // defines how the batch metadata is replicated across servers
    private BatchReplicationStrategy batchReplicationStrategy = BatchReplicationStrategy.NONE;

    public int getGroupThreadPoolSize() {
        return this.groupThreadPoolSize;
    }

    public CoordinatorOptions withGroupThreadPoolSize(int groupThreadPoolSize) {
        this.groupThreadPoolSize = groupThreadPoolSize;
        return this;
    }

    public CoordinatorOptions withNetworkBufferSize(int networkBufferSize) {
        this.networkBufferSize = networkBufferSize;
        return this;
    }

    public int getOsBufferSize(){
        return this.osBufferSize;
    }

    public CoordinatorOptions withOsBufferSize(int osBufferSize) {
        this.osBufferSize = osBufferSize;
        return this;
    }

    public int getNetworkBufferSize(){
        return this.networkBufferSize;
    }

    public CoordinatorOptions withNetworkSendTimeout(int networkSendTimeout) {
        this.networkSendTimeout = networkSendTimeout;
        return this;
    }

    public int getNetworkSendTimeout(){
        return this.networkSendTimeout;
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

    public long getBatchWindow() {
        return this.batchWindow;
    }

    public CoordinatorOptions withBatchWindow(long batchWindow) {
        this.batchWindow = batchWindow;
        return this;
    }

}
