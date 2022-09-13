package dk.ku.di.dms.vms.modb.common.schema.network.batch;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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
        buffer.put(Constants.BATCH_COMPLETE);
        buffer.putLong( batch );
        buffer.putInt( vmsIdentifier.getIdentifier().length() );
        buffer.put( vmsIdentifier.getIdentifier().getBytes(StandardCharsets.UTF_8) );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        int size = buffer.getInt();
        String vms = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );
        return new Payload(batch, vms);
    }

    public record Payload(
        long batch, String vms
    ){}

}
