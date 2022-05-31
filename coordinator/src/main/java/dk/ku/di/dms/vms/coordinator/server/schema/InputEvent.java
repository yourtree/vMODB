package dk.ku.di.dms.vms.coordinator.server.schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * The payload of an event
 */
public class InputEvent {

    // let's assume events come from the elected leader, so no need to encode the source server info
    // type | #events | <event name size> | event name | <event payload size> | event payload
    private static int headerSize = Byte.BYTES + Integer.BYTES + Integer.BYTES; //+ Byte.BYTES + Integer.BYTES;

    // so why not encoding the event identification (name) as part of the event payload?
    // to avoid having to decode the entire payload
    // I am assuming the received events show no errors. in a production system, it would be necessary to check for errors first

    public static void write(ByteBuffer buffer){

    }

    /**
     * Read name of the event in order to define to which VMS to send
     * @param buffer
     * @return
     */
    public static String readName(ByteBuffer buffer){
        int size = buffer.getInt();
        return new String(buffer.array(), Byte.BYTES + Integer.BYTES, Byte.BYTES + Integer.BYTES + size, StandardCharsets.UTF_8 );
    }

}
