package dk.ku.di.dms.vms.coordinator.socket;

import static dk.ku.di.dms.vms.coordinator.election.Constants.LEADER_REQUEST;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.coordinator.election.ElectionManager;
import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
    public void testBufferRead(){

        ByteBuffer buffer = ByteBuffer.allocate(128);

        byte[] hostBytes = "localhost".getBytes();

        buffer.put(LEADER_REQUEST);
        buffer.putInt( 80 );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );

        assert 1 == 1;

    }

    @Test
    public void leaderElectionTest() throws IOException, InterruptedException {

        AsynchronousServerSocketChannel serverSocket1 = AsynchronousServerSocketChannel.open();
        serverSocket1.bind( new InetSocketAddress(80) );

        AsynchronousServerSocketChannel serverSocket2 = AsynchronousServerSocketChannel.open();
        serverSocket2.bind( new InetSocketAddress(81) );

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 80 );
        ServerIdentifier serverEm2 = new ServerIdentifier( "localhost", 81 );
        ServerIdentifier serverEm3 = new ServerIdentifier( "localhost", 82 );

        AtomicReference<ServerIdentifier> leader = new AtomicReference<>();
        // leader.set(null);

        Map<Integer,ServerIdentifier> servers1 = new HashMap<>();
        servers1.put( serverEm2.hashCode(), serverEm2 );
        servers1.put( serverEm3.hashCode(), serverEm3 );

        Map<Integer,ServerIdentifier> servers2 = new HashMap<>();
        servers2.put( serverEm1.hashCode(), serverEm1 );
        servers2.put( serverEm3.hashCode(), serverEm3 );

        BlockingQueue<Byte> roundResult1 = new ArrayBlockingQueue<>(1);
        BlockingQueue<Byte> roundResult2 = new ArrayBlockingQueue<>(1);

        ElectionManager em1 = new ElectionManager(serverSocket1, null, Executors.newFixedThreadPool(2), serverEm1, leader, servers1, roundResult1 );
        ElectionManager em2 = new ElectionManager(serverSocket2, null, Executors.newFixedThreadPool(2), serverEm2, leader, servers2, roundResult2 );

        new Thread( em1 ).start();
        new Thread( em2 ).start();

        logger.info( "result 1: " + roundResult1.take() );
        logger.info( "result 2: " + roundResult2.take() );

        assert(true);

    }

}
