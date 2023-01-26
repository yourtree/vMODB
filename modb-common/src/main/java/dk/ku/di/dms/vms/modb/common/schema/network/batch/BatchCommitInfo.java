package dk.ku.di.dms.vms.modb.common.schema.network.batch;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;

/**
 * Payload that carries batch-commit information for given batch of messages.
 */
public final class BatchCommitInfo {

    public static final int size = 1 + (3 * Long.BYTES);

    public static void write(ByteBuffer buffer, long batch, long lastTidOfBatch, long previousBatch){
        buffer.put(Constants.BATCH_COMMIT_INFO);
        buffer.putLong( batch );
        buffer.putLong( lastTidOfBatch );
        buffer.putLong( previousBatch );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        long lastTidOfBatch = buffer.getLong();
        long previousBatch = buffer.getLong();
        return new Payload(batch, lastTidOfBatch, previousBatch);
    }

    public record Payload(
            long batch, long lastTidOfBatch, long previousBatch
    ){}

}
