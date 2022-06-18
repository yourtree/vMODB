package dk.ku.di.dms.vms.coordinator.server.schema.batch;

import dk.ku.di.dms.vms.coordinator.metadata.VmsIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.server.infra.Constants.BATCH_COMPLETE;

/**
 * A prepare request payload
 * Used to inform coordinator
 *
 * Using name because network address can change within a batch
 */
public final class BatchComplete {

    // commit   ----
    // type  | batch offset | size of string | string vms name
    private static final int headerSize = Byte.BYTES + Long.BYTES + Integer.BYTES; // + variable size

    public static void write(ByteBuffer buffer, long batch, VmsIdentifier vmsIdentifier){
        buffer.put(BATCH_COMPLETE);
        buffer.putLong( batch );
        buffer.putInt( vmsIdentifier.name.length() );
        buffer.put( vmsIdentifier.name.getBytes(StandardCharsets.UTF_8) );
    }

    public static BatchCompletePayload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        int size = buffer.getInt();
        String vms = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );
        return new BatchCompletePayload(batch, vms);
    }

    public record BatchCompletePayload (
        long batch, String vms
    ){}

}
