package dk.ku.di.dms.vms.coordinator.server.schema.batch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.server.infra.Constants.BATCH_REPLICATION;

/**
 * A batch-commit replication request payload
 *
 * Used to replicate across followers
 */
public final class BatchReplication {

    // commit   ----                            vms last tid
    // type  | batch offset  | size of string | string representing map of (vms,tid)
    private static final int headerSize = Byte.BYTES + Long.BYTES + Integer.BYTES; // + variable size

    public static void write(ByteBuffer buffer, long batch, String vmsTidMap){
        buffer.put(BATCH_REPLICATION);
        buffer.putLong( batch );
        buffer.putInt( vmsTidMap.length() );
        buffer.put( vmsTidMap.getBytes(StandardCharsets.UTF_8) );
    }

    public static BatchReplicationPayload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        int size = buffer.getInt();
        String map = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );
        return new BatchReplicationPayload(batch,map);
    }

    public record BatchReplicationPayload(
        long batch, String vmsTidMap
    ){}

}
