package dk.ku.di.dms.vms.coordinator.server.schema.batch;

import java.nio.ByteBuffer;

import static dk.ku.di.dms.vms.coordinator.server.infra.Constants.BATCH_COMMIT_REQUEST;

/**
 * A batch-abort request payload
 *
 * This only happens in case a new leader is elected while the previous leader has crashed in the middle of a batch
 * To avoid the overhead of replicating the transaction inputs on each sending, it is simpler to discard the entire batch
 * and restart with a new one
 */
public final class BatchAbortRequest {

    // commit  vms last tid
    // type  | offset       |
    private static final int headerSize = Byte.BYTES + Long.BYTES;

    // send the last tid (corresponding to the vms) and batch id
    public static void write(ByteBuffer buffer, long batch){

        buffer.put(BATCH_COMMIT_REQUEST);
        buffer.putLong( batch );

    }

    public static CommitRequestPayload read(ByteBuffer buffer){
        long tid = buffer.getLong();
        long batch = buffer.getLong();
        return new CommitRequestPayload(tid,batch);
    }

    public record CommitRequestPayload (
            long tid, long batch
    ){}

}
