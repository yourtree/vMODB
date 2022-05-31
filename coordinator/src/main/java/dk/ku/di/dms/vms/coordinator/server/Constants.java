package dk.ku.di.dms.vms.coordinator.server;

public class Constants {

    /**
     * Message identifiers
     */

    // from and to server nodes
    public static final byte HEARTBEAT = 0;

    /**
     * Transactional Events
     */
    public static final byte EVENT = 4;

    /**
     * Commit-related events
     */

    public static final byte COMMIT_REQUEST = 5;

    // a commit response can indicate whether a leadership no longer holds
    // after network problems(e.g., partitions or increased latency) and subsequent normalization
    public static final byte COMMIT_RESPONSE = 6;

    // coming from one or more VMSs in the same transaction
    public static final byte ABORT_REQUEST = 7;

    // so they can participate in next elections. send to the leader first, and he propagates to other servers
//    byte NEW_SERVER_REQUEST = 9;
//    byte NEW_SERVER_RESPONSE = 10;
    // not necessary, they will request a vote

}
