package dk.ku.di.dms.vms.coordinator.server.leader;

/**
 * Initialized with default values
 */
public class LeaderOptions {

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

    public int getHeartbeatSlack() {
        return heartbeatSlack;
    }

    public LeaderOptions withHeartbeatSlack(int heartbeatSlack) {
        this.heartbeatSlack = heartbeatSlack;
        return this;
    }

    public long getBatchWindow() {
        return batchWindow;
    }

    public LeaderOptions withBatchWindow(long batchWindow) {
        this.batchWindow = batchWindow;
        return this;
    }

    public long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public LeaderOptions withHeartbeatTimeout(long heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
        return this;
    }
}
