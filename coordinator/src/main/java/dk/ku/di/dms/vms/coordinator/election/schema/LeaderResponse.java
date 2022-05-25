package dk.ku.di.dms.vms.coordinator.election.schema;

import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.election.Constants.LEADER_RESPONSE;

/**
 * The payload of a request for info
 */
public class LeaderResponse {

    // type | port | size | <host address is variable> |
    private static final int headerSize = Byte.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, ServerIdentifier serverIdentifier){
        byte[] hostBytes = serverIdentifier.host.getBytes();
        buffer.put( LEADER_RESPONSE );
        buffer.putInt(serverIdentifier.port );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );
    }

    public static LeaderResponsePayload read(ByteBuffer buffer){

        LeaderResponsePayload payload = new LeaderResponsePayload();

        payload.port = buffer.getInt();

        int size = buffer.getInt();

        payload.host = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );

        return payload;
    }

    public static class LeaderResponsePayload {
        public String host;
        public int port;
    }

}
