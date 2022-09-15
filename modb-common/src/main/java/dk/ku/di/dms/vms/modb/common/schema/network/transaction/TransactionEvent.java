package dk.ku.di.dms.vms.modb.common.schema.network.transaction;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 *  The actual payload of what is sent to the VMSs
 */
public final class TransactionEvent {

    // this payload
    // message type | tid | last tid | size | event name | size | payload
    private static final int header = Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, Payload payload){

        buffer.put( Constants.EVENT );
        buffer.putLong( payload.tid );
        buffer.putLong( payload.lastTid );
        buffer.putLong( payload.batch );

        // size event name
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
        // size event name
        buffer.putInt( eventBytes.length );
        buffer.put( eventBytes );

        byte[] payloadBytes = payload.getBytes();
        buffer.putInt( payloadBytes.length );
        buffer.put( payloadBytes );

        return new Payload( tid, lastTid, batch, event, payload );

    }

    public static Payload read(ByteBuffer buffer){
        long tid = buffer.getLong();
        long lastTid = buffer.getLong();
        long batch = buffer.getLong();

        int eventSize = buffer.getInt();

        String eventName = extractStringFromByteBuffer( buffer, eventSize );

        int payloadSize = buffer.getInt();

        String payload = extractStringFromByteBuffer( buffer, payloadSize );

        // String payload = new String( buffer.array(), header + Integer.BYTES + eventSize, payloadSize, StandardCharsets.UTF_8 );

        return new Payload( tid, lastTid, batch, eventName, payload );
    }

    /**
     * This is the base class for representing the data transferred across the framework and the sidecar
     * It serves both for input and output
     */
    public static record Payload(
            long tid, long lastTid, long batch, String event, String payload
    ){}

    private static String extractStringFromByteBuffer(ByteBuffer buffer, int size){
        if(buffer.isDirect()){
            byte[] byteArray = new byte[size];
            for(int i = 0; i < size; i++){
                byteArray[i] = buffer.get();
            }
            return new String(byteArray, 0, size, StandardCharsets.UTF_8);
        } else {
            return new String(buffer.array(), buffer.position(), size, StandardCharsets.UTF_8);
        }
    }

}
