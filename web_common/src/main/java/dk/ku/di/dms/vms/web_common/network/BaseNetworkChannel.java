package dk.ku.di.dms.vms.web_common.network;

import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides protocol-agnostic functionalities
 * to network channels established among nodes
 *
 * - Identifying who is this server
 * - Identifying the known servers (whether they are online or not)
 * - Providing thread pool for (background/async) task processing
 * - Assignment of read/write buffers for each connection established
 * - Defining the strategy for writing and reading (e.g., batch/one-per-message/time-based)
 *
 * Why's that?
 * Network packets have a different granularity
 * Data systems send data messages. These are naturally deterministically numbered
 * Datagram and multicast channel for server and replicas may be the way to go for
 * performant data replication
 */
public abstract class BaseNetworkChannel extends SignalingStoppableRunnable implements INetworkProducer {

    protected NetworkNode me;

    // hash key is the hash of host and port
    protected Map<Integer, NetworkNode> servers;

    protected INetworkListener listener;

    public BaseNetworkChannel(NetworkNode me, Map<Integer, NetworkNode> servers, INetworkListener listener) {
        this.me = me;
        this.servers = servers;
        this.listener = listener;
    }

    // datagram does not have accept, it conectionless

    //    default void test() throws IOException {
//
//        MulticastSocket socket = new MulticastSocket(4321);
//
//        // https://docs.oracle.com/javase/7/docs/api/java/net/MulticastSocket.html
//
//    }

}
