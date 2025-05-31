package dk.ku.di.dms.vms.web_common.iouring;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dk.ku.di.dms.vms.web_common.iouring.util.ByteBufferUtil;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

public class IoUringSocketTest extends TestBase {

    @Test
    public void test_create_server_and_connect_should_succeed() {
        int port = randomPort();

        AtomicBoolean accepted = new AtomicBoolean(false);
        IoUringServerSocket serverSocket = new IoUringServerSocket(port);
        serverSocket.onAccept((ring, socket) -> {
            accepted.set(true);
            serverSocket.close();
        });
        serverSocket.onException(Exception::printStackTrace);

        AtomicBoolean connected = new AtomicBoolean(false);
        IoUringSocket socket = new IoUringSocket("127.0.0.1", port);
        socket.onException(Exception::printStackTrace);
        socket.onConnect(ring -> {
            connected.set(true);
            socket.close();
        });

        IoUring ioUring = new IoUring(TEST_RING_SIZE)
            .onException(Exception::printStackTrace)
            .queueAccept(serverSocket)
            .queueConnect(socket);

        attemptUntil(ioUring::execute, () -> accepted.get() && connected.get());

        ioUring.close();

        assertTrue(accepted.get(), "Server accepted connection");
        assertTrue(connected.get(), "Client connected");
    }

    @Test
    public void test_create_server_and_connect_with_wrong_port_should_produce_exception() {
        int port = randomPort();

        AtomicBoolean accepted = new AtomicBoolean(false);
        AtomicBoolean exceptionProduced = new AtomicBoolean(false);

        IoUringServerSocket serverSocket = new IoUringServerSocket(port);
        serverSocket.onAccept((ring, socket) -> accepted.set(true));
        serverSocket.onException(Exception::printStackTrace);

        IoUringSocket socket = new IoUringSocket("127.0.0.1", port + 1);
        socket.onException(ex -> exceptionProduced.set(true));

        IoUring ioUring = new IoUring(TEST_RING_SIZE)
            .onException(Exception::printStackTrace)
            .queueAccept(serverSocket)
            .queueConnect(socket);

        attemptUntil(ioUring::execute, exceptionProduced::get);

        ioUring.close();
        serverSocket.close();
        socket.close();

        assertFalse(accepted.get(), "Server accepted connection");
        assertTrue(exceptionProduced.get(), "Client to connect (wrong port)");
    }

    @Test
    public void test_create_server_connect_and_send_data_should_be_received() {
        int port = randomPort();
        String message = "Test over port " + randomPort();

        AtomicBoolean serverAccepted = new AtomicBoolean(false);
        AtomicBoolean serverSent = new AtomicBoolean(false);
        AtomicBoolean clientConnected = new AtomicBoolean(false);
        AtomicBoolean clientReceived = new AtomicBoolean(false);

        IoUringServerSocket serverSocket = new IoUringServerSocket(port);
        serverSocket.onException(Exception::printStackTrace);
        serverSocket.onAccept((ring, socket) -> {
            ByteBuffer testBuffer = ByteBufferUtil.wrapDirect(message);
            socket.onWrite(out -> socket.close());
            ring.queueWrite(socket, testBuffer);
            serverAccepted.set(true);
            serverSent.set(true);
        });

        IoUringSocket socket = new IoUringSocket("127.0.0.1", port);
        socket.onException(Exception::printStackTrace);
        socket.onConnect(ring -> {
            ring.queueRead(socket, ByteBuffer.allocateDirect(32));
            clientConnected.set(true);
        });
        socket.onRead(in -> {
            in.flip();
            String payload = StandardCharsets.UTF_8.decode(in).toString();
            if (payload.equals(message)) {
                clientReceived.set(true);
            }
            socket.close();
        });

        IoUring ioUring = new IoUring(TEST_RING_SIZE)
            .onException(Exception::printStackTrace)
            .queueAccept(serverSocket)
            .queueConnect(socket);

        attemptUntil(ioUring::execute, () ->
            serverAccepted.get() && clientConnected.get() && serverSent.get() && clientReceived.get());

        ioUring.close();

        assertTrue(serverAccepted.get(), "Server accepted connection");
        assertTrue(serverSent.get(), "Server sent data");
        assertTrue(clientConnected.get(), "Client connected");
        assertTrue(clientReceived.get(), "Client received data");
    }
}
