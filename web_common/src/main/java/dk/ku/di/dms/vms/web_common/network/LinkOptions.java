package dk.ku.di.dms.vms.web_common.network;

import dk.ku.di.dms.vms.web_common.buffer.BufferManager;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Link is the term used to describe the communication
 * channel between nodes in a network
 */
public class LinkOptions {

    public final int EIGHT_K = 8192;

    public enum MODE {
        PULL, // consumer pull for data
        PUSH // producer push for data, obeying the TCP congestion control policy + TPC channel configurations
    }

    private ConcurrentLinkedQueue<byte[]> buffer;

    private ByteBuffer offHeapBuffer = BufferManager.loanByteBuffer(32000);

    // header to send... is only the size of used buffer.
    // the consumer knows when the consumption is over by iterating over the objects

    int bufferSize;

    // the window on which the sender waits until sending
    long windowSize;

    int socketSendBufferSize;

    // https://docs.microsoft.com/en-us/dotnet/api/system.net.sockets.socket.sendbuffersize?view=net-6.0
    long sendTimeout;

    int sockerReceiveBufferSize;

    long receiveTimeout;

    long ttl;

    /**
     * https://en.wikipedia.org/wiki/Nagle%27s_algorithm
     *
     * Nagle's algorithm must be deactivated between Leader and followers since a heartbeat can be delayed
     */
    boolean naglesAlgorithm;

    boolean delayedAck;

    boolean keepAlive;

    // JVM options such as wrapper.java.additional.<n>=-XX:MaxDirectMemorySize=<size>

    private void simpleAlgorithmToEnqueueMessage(byte[] bytes){

        // while next message does not overflow current buffer size,
        // keep filling the off-heap buffer

    }

}
