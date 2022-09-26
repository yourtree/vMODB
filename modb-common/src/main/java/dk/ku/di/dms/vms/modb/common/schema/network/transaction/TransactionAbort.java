package dk.ku.di.dms.vms.modb.common.schema.network.transaction;

import dk.ku.di.dms.vms.modb.common.ByteUtils;
import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * An abort request is sent from a VMS to the leader.
 * The leader is then responsible for issuing abort request to all VMS participating in the given transaction.
 * The transaction must be stored in memory and discarded when the batch is committed.
 * This is necessary to get the participating VMSs.
 */
public class TransactionAbort {

    // type | tid  | size vms name | vms name
    private static final int headerSize = Byte.BYTES + Long.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, Payload payload) {
        buffer.put(Constants.TX_ABORT);
        buffer.putLong(payload.tid);
        buffer.putInt( payload.vms().length() );
        buffer.put( payload.vms().getBytes(StandardCharsets.UTF_8) );
    }

    public static void write(ByteBuffer buffer, VmsIdentifier vmsIdentifier, long tid){
        buffer.put(Constants.TX_ABORT);
        buffer.putLong(tid);
        buffer.putInt( vmsIdentifier.getIdentifier().length() );
        buffer.put( vmsIdentifier.getIdentifier().getBytes(StandardCharsets.UTF_8) );
    }

    public static Payload read(ByteBuffer buffer){
        long tid = buffer.getLong();
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return new Payload(tid, vms);
    }

    // a leader cannot issue new events (and batches of course) without receiving batch ACKs from all vms involved
    // so no need for further information in the payload
    public record Payload(
            long tid, String vms
    ) {}

}
