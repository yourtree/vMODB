package dk.ku.di.dms.vms.web_common.meta.schema.transaction;

import dk.ku.di.dms.vms.web_common.meta.VmsIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.web_common.meta.Constants.TX_ABORT;

/**
 * An abort request is sent from a VMS to the leader.
 * The leader is then responsible for issuing abort request to all VMS participating in the given transaction.
 * The transaction must be stored in memory and discarded when the batch is committed.
 * This is necessary to get the participating VMSs.
 */
public class TransactionAbort {

    // type | tid  | size vms name | vms name
    private static final int headerSize = Byte.BYTES + Long.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, TransactionAbortPayload payload) {
        buffer.put(TX_ABORT);
        buffer.putLong(payload.tid);
        buffer.putInt( payload.vms().length() );
        buffer.put( payload.vms().getBytes(StandardCharsets.UTF_8) );
    }

    public static void write(ByteBuffer buffer, VmsIdentifier vmsIdentifier, long tid){
        buffer.put(TX_ABORT);
        buffer.putLong(tid);
        buffer.putInt( vmsIdentifier.identifier.length() );
        buffer.put( vmsIdentifier.identifier.getBytes(StandardCharsets.UTF_8) );
    }

    public static TransactionAbortPayload read(ByteBuffer buffer){
        long tid = buffer.getLong();
        int size = buffer.getInt();
        String vms = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );
        return new TransactionAbortPayload(tid, vms);
    }

    // a leader cannot issue new events (and batches of course) without receiving batch ACKs from all vms involved
    // so no need for further information in the payload
    public record TransactionAbortPayload (
            long tid, String vms
    ) {}

}
