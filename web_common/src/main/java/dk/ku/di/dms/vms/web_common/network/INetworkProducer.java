package dk.ku.di.dms.vms.web_common.network;

import java.net.SocketAddress;
import java.nio.ByteBuffer;

public interface INetworkProducer {

    void send(SocketAddress node, ByteBuffer message);

    /**
     * Broadcast is waste of resources
     * Multicast can use the network efficiently to
     * avoid overhead on packet sending
     *
     * Join the default multicast group
     */
    void join();

    // void multicast(NetworkNode group, ByteBuffer message);
    void multicast(ByteBuffer message);

    void close();

    /*
     * Do we have a case a new node enters and an existing node has to contact it?
     * Not sure... probably connecting in the constructor is fine
     */
    // void connect(NetworkNode node);

}
