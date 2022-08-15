package dk.ku.di.dms.vms.web_common.network;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/**
 * Interface to abstract the use of the protocol (TCP and UDP)
 * and strategies for sending and receiving data over socket
 *
 * Inspired by the interfaces found in {@link java.net.http.WebSocket}
 *
 */
public interface INetworkListener {

    void onOpen();

    void onError(Throwable error);

    // it abstracts the accept function found in socket channel
    // when a new node connects, this function is called
    void onConnection(NetworkNode node);

    // on joining a multicast group
    // void onJoin();

    /**
     * The byte buffers eceived must be read and then discarded.
     * Must not be reused by the caller.
     */
    CompletionStage<Void> onMessage(ByteBuffer message);

}
