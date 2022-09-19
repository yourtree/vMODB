package dk.ku.di.dms.vms.modb.common.schema.network;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchAbortRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;

public final class Constants {

    /**
     * Message identifiers
     */

    // from and to server nodes
    public static final byte HEARTBEAT = 0;

    public static final byte PRESENTATION = 5;

    public static final byte CONSUMER_SET = 12;

    /**
     * Transaction-related Events
     */
    public static final byte EVENT = 4;

    // coming from one or more VMSs in the same transaction
    public static final byte TX_ABORT = 6;

    /**
     * Batch-commit-related events.
     * A batch never aborts.
     * Only individual transactions of the batch may abort.
     * So no need for 2-PC.
     * It works similarly as a snapshotting process in Flink
     */

    /**
     * all terminal VMSs that have participated in a batch must send this event
     * to coordinator in order to complete a batch
     * {@link BatchComplete}
    */
    public static final byte BATCH_COMPLETE = 7;

    /**
     * all terminal VMSs that have participated in a batch must send this event to
     * coordinator in order to complete a batch
     * {@link BatchAbortRequest}
     */
    public static final byte BATCH_REPLICATION = 8;

    public static final byte BATCH_REPLICATION_ACK = 12;

    // then the coordinator sends this message
    // VMSs after receiving this message snapshot (log) their states
    public static final byte BATCH_COMMIT_REQUEST = 9;

    // a commit response can indicate whether a leadership no longer holds
    // after network problems(e.g., partitions or increased latency) and subsequent normalization

    // VMSs respond the batch commit with this message... but can be avoided for decreased overhead
    // We assume a  service will eventually respond, even though there is a failure
     public static final byte BATCH_COMMIT_ACK = 10;

    /**
     *  This message is sent by a new elected leader to roll back all
     *  changes previously made by the previous ongoing batch
     *  {@link BatchAbortRequest}
     */
    public static final byte BATCH_ABORT_REQUEST = 11;

}
