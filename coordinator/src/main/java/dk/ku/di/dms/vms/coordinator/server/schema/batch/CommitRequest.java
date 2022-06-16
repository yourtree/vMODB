package dk.ku.di.dms.vms.coordinator.server.schema.batch;

import java.nio.ByteBuffer;

import static dk.ku.di.dms.vms.coordinator.server.infra.Constants.BATCH_COMMIT_REQUEST;

/**
 * A batch-commit request payload
 */
public final class CommitRequest {

    // commit                 respective vms last tid
    // type  | offset       | tid
    private static final int headerSize = Byte.BYTES + Long.BYTES + Long.BYTES;

    // send the last tid (corresponding to the vms) and batch id
    public static void write(ByteBuffer buffer, long batch, long tid){
        buffer.put(BATCH_COMMIT_REQUEST);
        buffer.putLong( batch );
        buffer.putLong( tid );
    }

    public static CommitRequestPayload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        long tid = buffer.getLong();
        return new CommitRequestPayload(tid,batch);
    }

    public record CommitRequestPayload (
            long tid, long batch
    ){}

}
