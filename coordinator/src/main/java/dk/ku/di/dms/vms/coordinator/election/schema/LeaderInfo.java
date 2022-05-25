package dk.ku.di.dms.vms.coordinator.election.schema;

import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.election.Constants.LEADER_INFO;

public class LeaderInfo {

    // type | port | size | <host address is variable>
    private static int fixedSize = Byte.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, ServerIdentifier serverIdentifier){
        byte[] hostBytes = serverIdentifier.host.getBytes();
        buffer.put(LEADER_INFO);
        buffer.putInt(serverIdentifier.port );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );
    }

    public static LeaderInfoPayload read(ByteBuffer buffer){

        int port = buffer.getInt();

        int size = buffer.getInt();

        // 1 + 8 + 4 = 8 + 4 =
        String host = new String( buffer.array(), fixedSize, size, StandardCharsets.UTF_8 );

        return new LeaderInfoPayload(  host, port );

    }

    public static class LeaderInfoPayload {
        public String host;
        public int port;

        public LeaderInfoPayload(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

}
