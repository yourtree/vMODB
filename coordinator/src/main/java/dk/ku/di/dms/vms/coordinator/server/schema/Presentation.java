package dk.ku.di.dms.vms.coordinator.server.schema;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;
import dk.ku.di.dms.vms.coordinator.metadata.VmsIdentifier;
import dk.ku.di.dms.vms.web_common.meta.VmsDataSchema;
import dk.ku.di.dms.vms.web_common.meta.VmsEventSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A presentation is a message that carries out the necessary info of a node
 */
public class Presentation {
    //                0 server 1 vms
    // message type | node type [0,1] | offset | port | size | <host address is variable> |
    private static final int commonHeader = Byte.BYTES + Byte.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

    // + size of data schema and event schema lists
    private static final int fixedVmsSize = commonHeader + Integer.BYTES + Integer.BYTES;

    public static VmsIdentifier readVms(ByteBuffer buffer, Gson gson){

        long offset = buffer.getLong();

        int port = buffer.getInt();

        int sizeHost = buffer.getInt();

        // 1 + 8 + 4 = 8 + 4 =
        String host = new String( buffer.array(), commonHeader, sizeHost, StandardCharsets.UTF_8 );

        // now read the rest
        int sizeDataSchema = buffer.getInt();
        int sizeEventSchema = buffer.getInt();

        String dataSchemaStr = new String( buffer.array(), fixedVmsSize, sizeDataSchema, StandardCharsets.UTF_8 );

        List<VmsDataSchema> dataSchema = gson.fromJson( dataSchemaStr, new TypeToken<List<VmsDataSchema>>(){}.getType() );

        String eventSchemaStr = new String( buffer.array(), fixedVmsSize + (sizeDataSchema * 4), sizeEventSchema, StandardCharsets.UTF_8 );

        List<VmsEventSchema> eventSchema = gson.fromJson( eventSchemaStr, new TypeToken<List<VmsEventSchema>>(){}.getType() );

        return new VmsIdentifier( dataSchema.get(0).virtualMicroservice, host, port, offset, dataSchema, eventSchema );

    }

    public static ServerIdentifier readServer(ByteBuffer buffer){

        long offset = buffer.getLong();

        int port = buffer.getInt();

        int size = buffer.getInt();

        // 1 + 8 + 4 = 8 + 4 =
        String host = new String( buffer.array(), commonHeader, size, StandardCharsets.UTF_8 );

        return new ServerIdentifier(  host, port, offset );

    }

}
