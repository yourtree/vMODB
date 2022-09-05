package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.web_common.meta.*;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static dk.ku.di.dms.vms.web_common.meta.Constants.PRESENTATION;

/**
 * A presentation is a message that carries out the necessary info of a node
 * Only for leader <--> VMS communication
 */
public final class Presentation {

    public static final byte YES = 1;
    public static final byte NO = 1;
    public static final byte SERVER_TYPE = 0;
    public static final byte VMS_TYPE = 1;

    //                                                      0 server 1 vms  if leader already have metadata
    //                                     message type | node type [0,1] | metadata bit | lastOffset | port | size host
    private static final int serverHeader = Byte.BYTES + Byte.BYTES +    Byte.BYTES +      Long.BYTES + Integer.BYTES + Integer.BYTES;

    //                0 server 1 vms
    // message type | node type [0,1] | last tid | last batch | port | size | <host address is variable> |
    private static final int vmsHeader = Byte.BYTES + Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

    // + size of data schema and event schema lists
    private static final int fixedVmsSize = vmsHeader + Integer.BYTES + Integer.BYTES;

    // for server consumption
    public static void writeServer(ByteBuffer buffer,
                                   ServerIdentifier serverIdentifier){
        buffer.put( Constants.PRESENTATION );
        buffer.put( SERVER_TYPE );

        buffer.putLong( serverIdentifier.lastOffset );
        buffer.putInt( serverIdentifier.port );

        byte[] host = serverIdentifier.host.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( host.length );
        buffer.put( host );
    }

    // for VMS consumption
    public static void writeServer(ByteBuffer buffer,
                                   ServerIdentifier serverIdentifier,
                                   String listOfVMSsToConnect,
                                   boolean includeMetadataOnAck
                                   ){
        buffer.put( Constants.PRESENTATION );
        buffer.put( SERVER_TYPE );

        buffer.put( includeMetadataOnAck ? YES : NO );

        buffer.putLong( serverIdentifier.lastOffset );

        buffer.putInt( serverIdentifier.port );

        byte[] host = serverIdentifier.host.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( host.length );
        buffer.put( host );

        // send the list of nodes this  VMS needs to connect to
        byte[] list = listOfVMSsToConnect.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( list.length );
        buffer.put(list);
    }

    public record PayloadFromServer(
            ServerIdentifier serverIdentifier,
            List<VmsIdentifier> consumers
    ){}

    public static void writeServer(ByteBuffer buffer, ServerIdentifier serverIdentifier, String listOfVMSsToConnect){
        writeServer(buffer,serverIdentifier, listOfVMSsToConnect, true);
    }

    // to be read by a VMS
    public static PayloadFromServer readServer(ByteBuffer buffer, IVmsSerdesProxy serdesProxy){

        long offset = buffer.getLong();

        int port = buffer.getInt();

        int hostSize = buffer.getInt();

        String host = new String( buffer.array(), serverHeader, hostSize, StandardCharsets.UTF_8 );

        int newOffset = serverHeader + hostSize;

        int consumerSetSize = buffer.getInt();

        String consumerVmsSetJSON = new String( buffer.array(), newOffset, consumerSetSize, StandardCharsets.UTF_8 );

        List<VmsIdentifier> consumerSet = serdesProxy.deserializeList( consumerVmsSetJSON );

        return new PayloadFromServer( new ServerIdentifier( host, port, offset ), consumerSet );

    }

    // to be read by a server (leader or follower)
    public static ServerIdentifier readServer(ByteBuffer buffer){

        long offset = buffer.getLong();

        int port = buffer.getInt();

        int size = buffer.getInt();

        String host = new String( buffer.array(), serverHeader, size, StandardCharsets.UTF_8 );

        return new ServerIdentifier( host, port, offset );

    }

    public static void writeVms(ByteBuffer buffer,
                                VmsIdentifier vms,
                                String dataSchema,
                                String eventSchema){

        buffer.put( Constants.PRESENTATION );
        buffer.put( VMS_TYPE );

        byte[] name = vms.getIdentifier().getBytes(StandardCharsets.UTF_8);
        buffer.putInt(name.length );
        buffer.put(name);

        buffer.putLong( vms.lastTid );
        buffer.putLong( vms.lastBatch );

        buffer.putInt( vms.port );

        byte[] host = vms.host.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( host.length );
        buffer.put( host );

        byte[] dataSchemaBytes = dataSchema.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( dataSchemaBytes.length );

        byte[] eventSchemaBytes = eventSchema.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( eventSchemaBytes.length );

        buffer.put( dataSchemaBytes );
        buffer.put( eventSchemaBytes );
    }

    public static VmsIdentifier readVms(ByteBuffer buffer, IVmsSerdesProxy serdesProxy){

        int sizeName = buffer.getInt();
        String vmsIdentifier = new String( buffer.array(), buffer.position(), sizeName, StandardCharsets.UTF_8 );

        long lastTid = buffer.getLong();
        long lastBatch = buffer.getLong();

        int port = buffer.getInt();

        int sizeHost = buffer.getInt();

        // 1 + 8 + 4 = 8 + 4 =
        String host = new String( buffer.array(), buffer.position(), sizeHost, StandardCharsets.UTF_8 );

        // now read the rest
        int sizeDataSchema = buffer.getInt();
        int sizeEventSchema = buffer.getInt();

        String dataSchemaStr = new String( buffer.array(), buffer.position(), sizeDataSchema, StandardCharsets.UTF_8 );

        Map<String,VmsDataSchema> dataSchema = serdesProxy.deserializeDataSchema( dataSchemaStr );

        String eventSchemaStr = new String( buffer.array(), buffer.position(), sizeEventSchema, StandardCharsets.UTF_8 );

        Map<String, VmsEventSchema> eventSchema = serdesProxy.deserializeEventSchema( eventSchemaStr );

        return new VmsIdentifier( host, port, vmsIdentifier, lastTid, lastBatch, dataSchema, eventSchema );

    }

}
