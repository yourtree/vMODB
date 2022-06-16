package dk.ku.di.dms.vms.coordinator.server.schema.transaction;

import dk.ku.di.dms.vms.coordinator.metadata.VmsIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.server.infra.Constants.TX_ABORT;

/**
 * An abort request is sent from a VMS to the leader.
 * The leader is then responsible for issuing abort request to all VMS participating in the given transaction.
 * The transaction must be stored in memory and discarded when the batch is committed.
 * This is necessary to get the participating VMSs.
 */
public class TransactionAbort {

    // type | tid | port | size | <host address is variable>
    private static final int headerSize = Byte.BYTES + Byte.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, VmsIdentifier vmsIdentifier, long tid){
        byte[] hostBytes = vmsIdentifier.host.getBytes();
        buffer.put(TX_ABORT);
        buffer.putLong(tid);
        buffer.putInt(vmsIdentifier.port );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );
    }

    public static TransactionAbortPayload read(ByteBuffer buffer){
        long tid = buffer.getLong();
        int port = buffer.getInt();
        int size = buffer.getInt();
        String host = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );
        return new TransactionAbortPayload(port, host, tid);
    }

    // a leader cannot issue new events (and batches of course) without receiving batch ACKs from all vms involved
    // so no need for further information in the payload
    public record TransactionAbortPayload (
            int port, String host, long tid
    ) {}

}
