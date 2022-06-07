package dk.ku.di.dms.vms.coordinator.server.schema;

import java.nio.ByteBuffer;

import static dk.ku.di.dms.vms.coordinator.server.Constants.HEARTBEAT;

/**
 *
 */
public class TransactionEvent {

    // this payload
    // message type | tid | size | event name | size | payload
    private static final int header = Byte.BYTES + Long.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, String eventName, String payload){

        //byte[] hostBytes = serverIdentifier.host.getBytes();
        buffer.put( HEARTBEAT );
        //buffer.putInt(serverIdentifier.port );
        //buffer.putInt( hostBytes.length );
        //buffer.put( hostBytes );

    }

}
