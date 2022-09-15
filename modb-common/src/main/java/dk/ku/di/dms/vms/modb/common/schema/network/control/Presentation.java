package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A presentation is a message that carries out the necessary info of a node
 * Only for leader <--> VMS communication
 */
public final class Presentation {

    public static final byte YES = 1;
    public static final byte NO = 1;
    public static final byte SERVER_TYPE = 10;
    public static final byte VMS_TYPE = 11;

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
            List<NetworkNode> consumers
    ){}

    public static void writeServer(ByteBuffer buffer, ServerIdentifier serverIdentifier, String listOfVMSsToConnect){
        writeServer(buffer,serverIdentifier, listOfVMSsToConnect, true);
    }

    // to be read by a VMS
    public static PayloadFromServer readServer(ByteBuffer buffer, IVmsSerdesProxy serdesProxy){

        long offset = buffer.getLong();

        int port = buffer.getInt();

        int hostSize = buffer.getInt();

        String host;
        if(buffer.isDirect()){
            byte[] byteArray = new byte[hostSize];
            for(int i = 0; i < hostSize; i++){
                byteArray[i] = buffer.get();
            }
            host = new String(byteArray, 0, hostSize, StandardCharsets.UTF_8);
        } else {
            host = new String(buffer.array(), serverHeader, hostSize, StandardCharsets.UTF_8);
        }

        int newOffset = serverHeader + hostSize;

        int consumerSetSize = buffer.getInt();
        List<NetworkNode> consumerSet;
        if(consumerSetSize > 0) {

            String consumerVmsSetJSON;
            if(buffer.isDirect()) {
                byte[] byteArray = new byte[consumerSetSize];
                for(int i = 0; i < consumerSetSize; i++){
                    byteArray[i] = buffer.get();
                }
                consumerVmsSetJSON = new String(byteArray, 0, consumerSetSize, StandardCharsets.UTF_8);
            }
            else {
                consumerVmsSetJSON = new String(buffer.array(), newOffset, consumerSetSize, StandardCharsets.UTF_8);
            }
            consumerSet = serdesProxy.deserializeList(consumerVmsSetJSON);

        } else {
            consumerSet = Collections.emptyList();
        }


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
        String vmsIdentifier = extractStringFromByteBuffer( buffer, sizeName );

        long lastTid = buffer.getLong();
        long lastBatch = buffer.getLong();

        int port = buffer.getInt();

        int sizeHost = buffer.getInt();

        String host;
        if(buffer.isDirect()){
            byte[] byteArray = new byte[sizeHost];
            for(int i = 0; i < sizeHost; i++){
                byteArray[i] = buffer.get();
            }
            host = new String(byteArray, 0, sizeHost, StandardCharsets.UTF_8);
        } else {
            host = new String(buffer.array(), buffer.position(), sizeHost, StandardCharsets.UTF_8);
        }

        // now read the rest
        int sizeDataSchema = buffer.getInt();
        int sizeEventSchema = buffer.getInt();

        String dataSchemaStr = extractStringFromByteBuffer(buffer, sizeDataSchema);

        Map<String,VmsDataSchema> dataSchema = serdesProxy.deserializeDataSchema( dataSchemaStr );

        String eventSchemaStr = extractStringFromByteBuffer(buffer, sizeEventSchema);

        Map<String, VmsEventSchema> eventSchema = serdesProxy.deserializeEventSchema( eventSchemaStr );

        return new VmsIdentifier( host, port, vmsIdentifier, lastTid, lastBatch, dataSchema, eventSchema );

    }

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
