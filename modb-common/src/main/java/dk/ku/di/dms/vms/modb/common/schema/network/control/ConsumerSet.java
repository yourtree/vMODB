package dk.ku.di.dms.vms.modb.common.schema.network.control;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public final class ConsumerSet {

    public static void write(ByteBuffer buffer,
                                   String mapStr){
        buffer.put( Constants.CONSUMER_SET );
        byte[] mapBytes = mapStr.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( mapBytes.length );
        buffer.put( mapBytes );
    }

    public static Map<String, List<IdentifiableNode>> read(ByteBuffer buffer, IVmsSerdesProxy proxy){
        int size = buffer.getInt();
        if(size > 0) {
            String consumerSet = ByteUtils.extractStringFromByteBuffer(buffer, size);
            return proxy.deserializeConsumerSet(consumerSet);
        }
        return null;
    }

}
