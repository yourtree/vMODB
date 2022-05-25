package dk.ku.di.dms.vms.coordinator.election.schema;

import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.election.Constants.VOTE_REQUEST;

/**
 * The payload of a request for info
 */
public class VoteRequest {

                                    // type |     offset |       port |      timestamp |      size | <host address is variable>
    private static int fixedSize = Byte.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, ServerIdentifier serverIdentifier){

        byte[] hostBytes = serverIdentifier.host.getBytes();

        buffer.put(VOTE_REQUEST);
        buffer.putLong( serverIdentifier.lastOffset );
        buffer.putInt(serverIdentifier.port );
        buffer.putLong(serverIdentifier.timestamp );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );

    }

    public static ServerIdentifier read(ByteBuffer buffer){

        long offset = buffer.getLong();

        int port = buffer.getInt();

        long timestamp = buffer.getLong();

        int size = buffer.getInt();

        // 1 + 8 + 4 = 8 + 4 =
        String host = new String( buffer.array(), fixedSize, size, StandardCharsets.UTF_8 );

        return new ServerIdentifier(  host, port, offset, timestamp );

    }


}
