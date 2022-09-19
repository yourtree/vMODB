package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.ByteUtils;
import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class ConsumerSet {

    public static void write(ByteBuffer buffer,
                                   String mapStr){
        buffer.put( Constants.CONSUMER_SET );
        byte[] mapBytes = mapStr.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( mapBytes.length );
        buffer.put( mapBytes );
    }

    public static Map<String, NetworkNode> read(ByteBuffer buffer, IVmsSerdesProxy proxy){
        int size = buffer.getInt();
        if(size > 0) {
            String consumerSet = ByteUtils.extractStringFromByteBuffer(buffer, size);
            return proxy.deserializeMap(consumerSet);
        }
        return null;
    }

}
