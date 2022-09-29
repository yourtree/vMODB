package dk.ku.di.dms.vms.modb.common.schema.network.batch;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;

/**
 * Payload that carries batch-commit information for given batch of messages.
 */
public final class BatchCommitInfo {

    public static final int size = 1 + (2 * Long.BYTES);

    public static void write(ByteBuffer buffer, long batch, long tid){
        buffer.put(Constants.BATCH_COMMIT_INFO);
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
