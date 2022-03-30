package dk.ku.di.dms.vms.sdk.core.client.net;

import dk.ku.di.dms.vms.modb.common.event.IEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


/**
 * https://crunchify.com/java-nio-non-blocking-io-with-server-client-example-java-nio-bytebuffer-and-channels-selector-java-nio-vs-io/
 */
public class NIOClient {

    public static void start() throws IOException {

        InetSocketAddress address = new InetSocketAddress("localhost", 1111);

        SocketChannel client = SocketChannel.open(address);

        TransactionalEvent tr = new TransactionalEvent(123, new IEvent() {
            @Override
            public String toString() {
                return "TESTE";
            }
        });

        String eventBody = "{ tid: 123 }";
        byte[] eventByte = eventBody.getBytes();

        ByteBuffer buffer = ByteBuffer.wrap(eventByte);
        client.write(buffer);

        client.close();

    }

}
