package dk.ku.di.dms.vms.coordinator.server.schema.transaction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.server.infra.Constants.EVENT;

/**
 *  The actual payload of what is sent to the VMSs
 *  Inside the VMS, this event becomes a {@link dk.ku.di.dms.vms.modb.common.event.TransactionalEvent}
 */
public final class TransactionEvent {

    // this payload
    // message type | tid | last tid | size | event name | size | payload
    private static final int header = Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, TransactionEventPayload payload){

        buffer.put( EVENT );
        buffer.putLong( payload.tid );
        buffer.putLong( payload.lastTid );
        buffer.putLong( payload.batch );

        byte[] eventBytes = payload.eventName.getBytes();
        // size event name
        buffer.putInt( eventBytes.length );
        buffer.put( eventBytes );

        byte[] payloadBytes = payload.payload.getBytes();
        buffer.putInt( payloadBytes.length );
        buffer.put( payloadBytes );

    }

    public static TransactionEventPayload write(ByteBuffer buffer, long tid, long lastTid, long batch, String eventName, String payload){

        buffer.put( EVENT );
        buffer.putLong( tid );
        buffer.putLong( lastTid );
        buffer.putLong( batch );

        byte[] eventBytes = eventName.getBytes();
        // size event name
        buffer.putInt( eventBytes.length );
        buffer.put( eventBytes );

        byte[] payloadBytes = payload.getBytes();
        buffer.putInt( payloadBytes.length );
        buffer.put( payloadBytes );

        return new TransactionEventPayload( tid, lastTid, batch, eventName, payload );

    }

    public static TransactionEventPayload read(ByteBuffer buffer){
        long tid = buffer.getLong();
        long lastTid = buffer.getLong();
        long batch = buffer.getLong();

        int eventSize = buffer.getInt();

        String eventName = new String( buffer.array(), header, eventSize, StandardCharsets.UTF_8 );

        int payloadSize = buffer.getInt();

        String payload = new String( buffer.array(), header + Integer.BYTES + eventSize, payloadSize, StandardCharsets.UTF_8 );

        return new TransactionEventPayload( tid, lastTid, batch, eventName, payload );
    }

    public static record TransactionEventPayload (
        long tid, long lastTid, long batch, String eventName, String payload
    ){}

}
