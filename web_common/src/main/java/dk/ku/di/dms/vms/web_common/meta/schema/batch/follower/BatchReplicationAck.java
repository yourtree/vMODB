package dk.ku.di.dms.vms.web_common.meta.schema.batch;

import java.nio.ByteBuffer;

import static dk.ku.di.dms.vms.web_common.meta.Constants.BATCH_REPLICATION_ACK;

/**
 * A batch-commit replication request payload
 *
 * Used to replicate across followers
 */
public final class BatchReplicationAck {

    // commit   ----
    // type  | batch offset  |
    private static final int headerSize = Byte.BYTES + Long.BYTES;

    public static void write(ByteBuffer buffer, long batch){
        buffer.put(BATCH_REPLICATION_ACK);
        buffer.putLong( batch );
    }

    public static BatchReplicationAckPayload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        return new BatchReplicationAckPayload(batch);
    }

    public record BatchReplicationAckPayload(
        long batch
    ){}

}
