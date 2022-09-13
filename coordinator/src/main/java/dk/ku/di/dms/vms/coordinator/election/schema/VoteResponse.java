package dk.ku.di.dms.vms.coordinator.election.schema;

import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.election.Constants.VOTE_RESPONSE;

/**
 * The payload of a request for info
 */
public class VoteResponse {

    // type | response | port | size | <host address is variable>
    private static final int headerSize = Byte.BYTES + Byte.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, NetworkNode serverIdentifier, boolean response){
        byte[] hostBytes = serverIdentifier.host.getBytes();
        buffer.put( VOTE_RESPONSE );
        buffer.putInt( response ? 1 : 0 );
        buffer.putInt(serverIdentifier.port );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );
    }

    public static Payload read(ByteBuffer buffer){

        // requires 4 bytes, reads from 1 to 4.
        boolean response = buffer.getInt() == 1;

        int port = buffer.getInt();

        int size = buffer.getInt();

        String host = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );

        return new Payload(host, port, response);
    }

    public static class Payload {

        public String host;
        public int port;
        public boolean response;

        public Payload(String host, int port, boolean response) {
            this.host = host;
            this.port = port;
            this.response = response;
        }
    }

}
