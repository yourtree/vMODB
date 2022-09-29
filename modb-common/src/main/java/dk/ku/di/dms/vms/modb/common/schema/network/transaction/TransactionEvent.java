package dk.ku.di.dms.vms.modb.common.schema.network.transaction;

import dk.ku.di.dms.vms.modb.common.ByteUtils;
import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;

/**
 *  The actual payload of what is sent to the VMSs
 */
public final class TransactionEvent {

    // this payload
    // message type | tid | last tid | size | event name | size | payload
    private static final int header = Long.BYTES + Long.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, Payload payload){
        buffer.put( Constants.EVENT );
        writeNoType(buffer, payload);
    }

    public static void writeNoType(ByteBuffer buffer, Payload payload){

        buffer.putLong( payload.tid );
        buffer.putLong( payload.lastTid );
        buffer.putLong( payload.batch );

        byte[] eventBytes = payload.event.getBytes();
        buffer.putInt( eventBytes.length );
        buffer.put( eventBytes );

        byte[] payloadBytes = payload.payload.getBytes();
        buffer.putInt( payloadBytes.length );
        buffer.put( payloadBytes );

    }

    public static Payload write(ByteBuffer buffer, long tid, long lastTid, long batch, String event, String payload){

        buffer.put( Constants.EVENT );
        buffer.putLong( tid );
        buffer.putLong( lastTid );
        buffer.putLong( batch );

        byte[] eventBytes = event.getBytes();
        buffer.putInt( eventBytes.length );
        buffer.put( eventBytes );

        byte[] payloadBytes = payload.getBytes();
        buffer.putInt( payloadBytes.length );
        buffer.put( payloadBytes );

        return new Payload( tid, lastTid, batch, event, payload, 1 + (Long.BYTES * 3) + eventBytes.length + payloadBytes.length );

    }

    public static Payload read(ByteBuffer buffer){
        long tid = buffer.getLong();
        long lastTid = buffer.getLong();
        long batch = buffer.getLong();

        int eventSize = buffer.getInt();

        String eventName = ByteUtils.extractStringFromByteBuffer( buffer, eventSize );

        int payloadSize = buffer.getInt();

        String payload = ByteUtils.extractStringFromByteBuffer( buffer, payloadSize );

        return new Payload( tid, lastTid, batch, eventName, payload, (Long.BYTES * 3) + eventSize + payloadSize );
    }

    /**
     * This is the base class for representing the data transferred across the framework and the sidecar
     * It serves both for input and output
     */
    public static record Payload(
            long tid, long lastTid, long batch, String event, String payload, int totalSize
    ){}

    public static Payload of(long tid, long lastTid, long batch, String event, String payload){
        // considering UTF-8
        // https://www.quora.com/How-many-bytes-can-a-string-hold
        int eventBytes = (event.length() * 8) / 8;
        int payloadBytes = (payload.length() * 8) / 8;
        return new Payload(tid, lastTid, batch, event, payload, header + eventBytes + payloadBytes);
    }

}
