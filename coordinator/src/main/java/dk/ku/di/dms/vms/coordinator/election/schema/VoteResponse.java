package dk.ku.di.dms.vms.coordinator.election.schema;

import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.election.Constants.VOTE_RESPONSE;

/**
 * The payload of a request for info
 */
public class VoteResponse {

    // type | response | port | size | <host address is variable>
    private static final int headerSize = Byte.BYTES + Byte.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, ServerIdentifier serverIdentifier, boolean response){

        byte[] hostBytes = serverIdentifier.host.getBytes();

        buffer.put( VOTE_RESPONSE );
        buffer.putInt( response ? 1 : 0 );
        buffer.putInt(serverIdentifier.port );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );

    }

    public static VoteResponsePayload read(ByteBuffer buffer){

        VoteResponsePayload payload = new VoteResponsePayload();

        // requires 4 bytes, reads from 1 to 4.
        payload.response = buffer.getInt() == 1;

        payload.port = buffer.getInt();

        int size = buffer.getInt();

        payload.host = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );

        return payload;
    }

    public static class VoteResponsePayload {
        public boolean response;
        public String host;
        public int port;
    }

}
