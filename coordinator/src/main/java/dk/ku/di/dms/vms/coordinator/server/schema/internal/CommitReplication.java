package dk.ku.di.dms.vms.coordinator.server.schema.internal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.server.Constants.COMMIT_REQUEST;

/**
 * A batch-commit replication request payload
 */
public final class CommitReplication {

    // commit   ----                            vms last tid
    // type | batch offset  | size of string | string representing map of (vms,tid)
    private static final int headerSize = Byte.BYTES + Long.BYTES + Integer.BYTES; // + variable size

    public static void write(ByteBuffer buffer, long batch, String vmsTidMap){
        buffer.put( COMMIT_REQUEST );
        buffer.putLong( batch );
        buffer.putInt( vmsTidMap.length() );
        buffer.put( vmsTidMap.getBytes(StandardCharsets.UTF_8) );
    }

    public static CommitReplicationPayload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        int size = buffer.getInt();
        String map = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );
        return new CommitReplicationPayload(batch,map);
    }

    public record CommitReplicationPayload (
            long batch, String vmsTidMap
    ){}

}
