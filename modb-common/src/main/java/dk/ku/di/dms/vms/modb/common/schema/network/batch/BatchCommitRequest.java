package dk.ku.di.dms.vms.modb.common.schema.network.batch;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;

/**
 * A batch-commit request payload
 */
public final class BatchCommitRequest {

    public static final int size = 1 + (2 * Long.BYTES);

    // send the last tid (corresponding to the vms) and batch id
    public static void write(ByteBuffer buffer, long batch, long tid){
        buffer.put(Constants.BATCH_COMMIT_REQUEST);
        buffer.putLong( batch );
        buffer.putLong( tid );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        long tid = buffer.getLong();
        return new Payload(tid,batch);
    }

    public record Payload(
            long tid, long batch
    ){}

}
