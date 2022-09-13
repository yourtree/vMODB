package dk.ku.di.dms.vms.modb.common.schema.network.batch;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A batch-commit response payload
 *
 * The concept of aborting a batch-commit does not exist.
 *
 * For avoiding messages (optimizing the protocol), this message
 * can be dropped (ignored) or simply marshalled with another.
 */
public final class BatchCommitResponse {

    // type | batch offset |  size of string | string vms name
    private static final int headerSize = Byte.BYTES + Byte.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, long batch, VmsIdentifier vmsIdentifier){
        buffer.put(Constants.BATCH_COMMIT_ACK);
        buffer.putLong( batch );
        byte[] nameBytes = vmsIdentifier.getIdentifier().getBytes(StandardCharsets.UTF_8);
        buffer.putInt( nameBytes.length );
        buffer.put( nameBytes );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        int size = buffer.getInt();
        String vms = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );
        return new Payload(batch, vms);
    }

    // a leader cannot issue new events (and batches of course) without receiving batch ACKs from all vms involved
    // so no need for further information in the payload
    public record Payload(
            long batch, String vms
    ) {}

}
