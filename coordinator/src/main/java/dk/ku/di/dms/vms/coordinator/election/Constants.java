package dk.ku.di.dms.vms.coordinator.election;

public class Constants {

    /**
     * Message identifiers
     */

    // from VMSs
    public static final byte HEARTBEAT = 0;

    // a server requesting a vote
    public static final byte VOTE_REQUEST = 1;

    // a server responding a request
    public static final byte VOTE_RESPONSE = 2;

    // a server claims to be the leader
    public static final byte LEADER_REQUEST = 3;

    // a server responds whether it has accepted the server as the leader
    public static final byte LEADER_RESPONSE = 4;

    public static final byte COMMIT_REQUEST = 5;
    public static final byte COMMIT_RESPONSE = 6;

    // coming from one or more VMSs in the same transaction
    public static final byte ABORT_REQUEST = 7;

    // so they can participate in next elections. send to the leader first, and he propagates to other servers
//    byte NEW_SERVER_REQUEST = 9;
//    byte NEW_SERVER_RESPONSE = 10;
    // not necessary, they will request a vote

    // in case a leader has already been established and a server requests a vote
    public static final byte LEADER_INFO = 8;

    /**
     * Protocol result
     */
//    public static final byte LEADER = 10;
//    public static final byte FOLLOWER = 11;

    public static final byte FINISHED = 12;
    public static final byte NO_RESULT = 12;

}
