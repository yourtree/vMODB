package dk.ku.di.dms.vms.modb.common.schema.network.transaction;

import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;
import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;

/**
 *  The actual payload of what is sent to the VMSs
 */
public final class TransactionEvent {

    // this payload
    // message type | tid | batch | size | event name | size | payload
    private static final int fixedLength = 1 + (2 * Long.BYTES) + (3 *  Integer.BYTES);

    public static void write(ByteBuffer buffer, Payload payload){
        buffer.put( Constants.EVENT );
        buffer.putLong( payload.tid );
        buffer.putLong( payload.batch );
        byte[] eventBytes = payload.event.getBytes();
        buffer.putInt( eventBytes.length );
        buffer.put( eventBytes );
        byte[] payloadBytes = payload.payload.getBytes();
        buffer.putInt( payloadBytes.length );
        buffer.put( payloadBytes );
        byte[] precedenceBytes = payload.precedenceMap.getBytes();
        buffer.putInt( precedenceBytes.length );
        buffer.put( precedenceBytes );
    }

    public static Payload write(ByteBuffer buffer, long tid, long batch, String event, String payload, String precedenceMap){
        buffer.put( Constants.EVENT );
        buffer.putLong( tid );
        buffer.putLong( batch );
        byte[] eventBytes = event.getBytes();
        buffer.putInt( eventBytes.length );
        buffer.put( eventBytes );
        byte[] payloadBytes = payload.getBytes();
        buffer.putInt( payloadBytes.length );
        buffer.put( payloadBytes );
        byte[] precedenceBytes = precedenceMap.getBytes();
        buffer.putInt( precedenceBytes.length );
        buffer.put( precedenceBytes );
        return new Payload( tid, batch, event, payload, precedenceMap,
                (Long.BYTES * 3) + eventBytes.length + payloadBytes.length + precedenceBytes.length );
    }

    public static Payload read(ByteBuffer buffer){
        long tid = buffer.getLong();
        long batch = buffer.getLong();
        int eventSize = buffer.getInt();
        String event = ByteUtils.extractStringFromByteBuffer( buffer, eventSize );
        int payloadSize = buffer.getInt();
        String payload = ByteUtils.extractStringFromByteBuffer( buffer, payloadSize );
        int precedenceSize = buffer.getInt();
        String precedenceMap = ByteUtils.extractStringFromByteBuffer( buffer, precedenceSize );
        return new Payload( tid, batch, event, payload, precedenceMap, (Long.BYTES * 3) + eventSize + payloadSize + precedenceSize );
    }

    /**
     * This is the base class for representing the data transferred across the framework and the sidecar
     * It serves both for input and output
     * Why total size? to know the size beforehand, before inserting into the byte buffer
     * otherwise would need further controls...
     */
    public record Payload(
            long tid, long batch, String event, String payload, String precedenceMap, int totalSize
    ){}

    public static Payload of(long tid, long batch, String event, String payload, String precedenceMap){
        // considering UTF-8
        // https://www.quora.com/How-many-bytes-can-a-string-hold
        int eventBytes = event.length();
        int payloadBytes = payload.length();
        int precedenceMapBytes = precedenceMap.length();
        return new Payload(tid, batch, event, payload, precedenceMap, fixedLength + eventBytes + payloadBytes + precedenceMapBytes);
    }

}