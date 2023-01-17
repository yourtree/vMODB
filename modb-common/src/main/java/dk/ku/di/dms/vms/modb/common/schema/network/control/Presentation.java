package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.ByteUtils;
import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

/**
 * A presentation is a message that carries out the necessary info of a node
 * Only for leader <--> VMS communication
 */
public final class Presentation {

    public static final byte YES = 1;
    public static final byte NO = 1;
    public static final byte SERVER_TYPE = 10;
    public static final byte VMS_TYPE = 11;
    public static final byte CLIENT = 12;

    //                                                      0 server 1 vms  if leader already have metadata
    //                                     message type | node type [0,1] | metadata bit | lastOffset | port | size host
    private static final int serverHeader = Byte.BYTES + Byte.BYTES +    Byte.BYTES +      Long.BYTES + Integer.BYTES + Integer.BYTES;

    //                0 server 1 vms
    // message type | node type [0,1] | last tid | last batch | port | size | <host address is variable> |
    private static final int vmsHeader = Byte.BYTES + Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

    // + size of data schema and event schema lists
    private static final int fixedVmsSize = vmsHeader + Integer.BYTES + Integer.BYTES;

    public static void writeClient(ByteBuffer buffer, String table){
        buffer.put( Constants.PRESENTATION );
        buffer.put( CLIENT );

        byte[] host = table.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( host.length );
        buffer.put( host );
    }

    public static String readClient(ByteBuffer buffer){
        int stringSize = buffer.getInt();

        String table;
        if(buffer.isDirect()){
            byte[] byteArray = new byte[stringSize];
            for(int i = 0; i < stringSize; i++){
                byteArray[i] = buffer.get();
            }
            table = new String(byteArray, 0, stringSize, StandardCharsets.UTF_8);
        } else {
            table = new String(buffer.array(), serverHeader, stringSize, StandardCharsets.UTF_8);
        }
        return table;
    }


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
                                   boolean includeMetadataOnAck){
        buffer.put( Constants.PRESENTATION );
        buffer.put( SERVER_TYPE );

        buffer.put( includeMetadataOnAck ? YES : NO );

        buffer.putLong( serverIdentifier.lastOffset );

        buffer.putInt( serverIdentifier.port );

        byte[] host = serverIdentifier.host.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( host.length );
        buffer.put( host );

    }

    public static ServerIdentifier readServer(ByteBuffer buffer){

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

        return new ServerIdentifier( host, port, offset );

    }

    public static void writeVms(ByteBuffer buffer,
                                NetworkNode node,
                                String vmsIdentifier,
                                long lastTid,
                                long lastBatch) {

        buffer.put( Constants.PRESENTATION );
        buffer.put( VMS_TYPE );

        byte[] name = vmsIdentifier.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(name.length );
        buffer.put(name);

        buffer.putLong( lastTid );
        buffer.putLong( lastBatch );

        buffer.putInt( node.port );

        byte[] host = node.host.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( host.length );
        buffer.put( host );
    }

    /**
     * Method is agnostic to the vms class
     */
    public static void writeVms(ByteBuffer buffer,
                                NetworkNode node,
                                String vmsIdentifier,
                                long lastTid,
                                long lastBatch,
                                String dataSchema,
                                String inputEventSchema,
                                String outputEventSchema){

        writeVms(buffer,node,vmsIdentifier,lastTid,lastBatch);

        byte[] dataSchemaBytes = dataSchema.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( dataSchemaBytes.length );
        buffer.put( dataSchemaBytes );

        byte[] inputEventSchemaBytes = inputEventSchema.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( inputEventSchemaBytes.length );
        buffer.put( inputEventSchemaBytes );

        byte[] outputEventSchemaBytes = outputEventSchema.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( outputEventSchemaBytes.length );
        buffer.put( outputEventSchemaBytes );
    }

    public static VmsIdentifier readVms(ByteBuffer buffer, IVmsSerdesProxy serdesProxy){

        int sizeName = buffer.getInt();
        String vmsIdentifier = ByteUtils.extractStringFromByteBuffer( buffer, sizeName );

        long lastTid = buffer.getLong();
        long lastBatch = buffer.getLong();

        int port = buffer.getInt();

        int sizeHost = buffer.getInt();

        String host = ByteUtils.extractStringFromByteBuffer(buffer, sizeHost);

        // now read the rest
        int sizeDataSchema = buffer.getInt();

        String dataSchemaStr = ByteUtils.extractStringFromByteBuffer(buffer, sizeDataSchema);

        Map<String, VmsDataSchema> dataSchema = serdesProxy.deserializeDataSchema( dataSchemaStr );

        int sizeInputEventSchema = buffer.getInt();

        String inputEventSchemaStr = ByteUtils.extractStringFromByteBuffer(buffer, sizeInputEventSchema);

        Map<String, VmsEventSchema> inputEventSchema = serdesProxy.deserializeEventSchema( inputEventSchemaStr );

        int sizeOutputEventSchema = buffer.getInt();

        String outputEventSchemaStr = ByteUtils.extractStringFromByteBuffer(buffer, sizeOutputEventSchema);

        Map<String, VmsEventSchema> outputEventSchema = serdesProxy.deserializeEventSchema( outputEventSchemaStr );

        return new VmsIdentifier( host, port, vmsIdentifier, lastTid, lastBatch, dataSchema, inputEventSchema, outputEventSchema );

    }

    public static Set<String> readQueuesToSubscribeTo(ByteBuffer buffer, IVmsSerdesProxy serdesProxy){
        int queueSetSize = buffer.getInt();
        String setStr = ByteUtils.extractStringFromByteBuffer(buffer, queueSetSize);
        return serdesProxy.deserializeSet(setStr);
    }

    public static void writeQueuesToSubscribeTo(ByteBuffer buffer, Set<String> queues, IVmsSerdesProxy serdesProxy){
        buffer.put(YES);
        String queuesStr = serdesProxy.serializeSet(queues);
        byte[] queuesStrBytes = queuesStr.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( queuesStrBytes.length );
        buffer.put( queuesStrBytes );
    }

}
