package dk.ku.di.dms.vms.coordinator.server.schema.batch;

import dk.ku.di.dms.vms.coordinator.metadata.VmsIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.server.infra.Constants.*;

/**
 * A batch-commit response payload
 *
 * The concept of aborting a batch-commit does not exist
 */
public final class BatchCommitResponse {

    // type | offset | port | size | <host address is variable>
    private static final int headerSize = Byte.BYTES + Byte.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, long offset, VmsIdentifier vmsIdentifier){
        byte[] hostBytes = vmsIdentifier.host.getBytes();
        buffer.put( BATCH_COMMIT_RESPONSE );
        buffer.putLong( offset );
        buffer.putInt(vmsIdentifier.port );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );
    }

    public static CommitResponsePayload read(ByteBuffer buffer){
        long offset = buffer.getLong();
        int port = buffer.getInt();
        int size = buffer.getInt();
        String host = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );
        return new CommitResponsePayload(port, host, offset);
    }

    // a leader cannot issue new events (and batches of course) without receiving batch ACKs from all vms involved
    // so no need for further information in the payload
    public record CommitResponsePayload (
        int port, String host, long offset
    ) {}

}
