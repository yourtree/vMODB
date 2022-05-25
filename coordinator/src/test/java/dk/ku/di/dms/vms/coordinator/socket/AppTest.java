package dk.ku.di.dms.vms.coordinator.socket;

import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.coordinator.election.ElectionManager;
import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Unit test for simple App.
 */
public class AppTest 
{

    protected final Logger logger = Logger.getLogger(this.getClass().getName());

    @Test
    public void shouldAnswerWithTrue() throws IOException, InterruptedException {

        AsynchronousServerSocketChannel serverSocket1 = AsynchronousServerSocketChannel.open();
        serverSocket1.bind( new InetSocketAddress(80) );

        AsynchronousServerSocketChannel serverSocket2 = AsynchronousServerSocketChannel.open();
        serverSocket2.bind( new InetSocketAddress(81) );

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 80 );
        ServerIdentifier serverEm2 = new ServerIdentifier( "localhost", 81 );
        ServerIdentifier serverEm3 = new ServerIdentifier( "localhost", 82 );

        AtomicReference<ServerIdentifier> leader = new AtomicReference<>();
        // leader.set(null);

        Map<Integer,ServerIdentifier> servers = new HashMap<>();
        servers.put( serverEm1.hashCode(), serverEm1 );
        servers.put( serverEm2.hashCode(), serverEm2 );
        servers.put( serverEm3.hashCode(), serverEm3 );

        BlockingQueue<Byte> roundResult1 = new ArrayBlockingQueue<>(1);
        BlockingQueue<Byte> roundResult2 = new ArrayBlockingQueue<>(2);

        ElectionManager em1 = new ElectionManager(serverSocket1, null, Executors.newFixedThreadPool(2), serverEm1, leader, servers, roundResult1 );
        ElectionManager em2 = new ElectionManager(serverSocket2, null, Executors.newFixedThreadPool(2), serverEm1, leader, servers, roundResult2 );

        new Thread( em1 ).start();
        new Thread( em2 ).start();

        roundResult1.take();
        roundResult2.take();

        assert(true);

    }


}
