package dk.ku.di.dms.vms.sidecar;

import dk.ku.di.dms.vms.sidecar.server.VMSServer;
import org.junit.Test;

import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static java.util.logging.Logger.getLogger;

public class WebSocketServerTest {

    private static final Logger logger = getLogger(WebSocketServerTest.class.getName());

    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    @Test
    public void test() {

        // create a thread...
        VMSServer vmsServer = new VMSServer();
        Thread t0 = new Thread(vmsServer);
        t0.start();


        Thread t1 = new Thread( () -> {

            VMSClient listener = new VMSClient();

            HttpClient client = HttpClient.newBuilder()
                    .executor(executor)
                    .build();

            CompletableFuture<WebSocket> cf = client.newWebSocketBuilder()//.connectTimeout(Duration.ofSeconds(10))
                    .buildAsync(URI.create("ws://127.0.0.1:80"), listener);

            try {
                WebSocket ws = cf.get();
                System.out.println("Web socket client created");

//                ByteBuffer bb = ByteBuffer.wrap( NetworkUtils.encode(" Sending from client SOMETHING>>>>>>>> ") );
//
//                CompletableFuture<WebSocket> msgCf = ws.sendBinary(bb,false);
//
//                WebSocket ws1 = msgCf.get();
//
//                System.out.println("Message sent to server");
//
//                System.out.println("Sending another message to server...");
//
//                bb = ByteBuffer.wrap( NetworkUtils.encode(" Sending from client SOMETHING2>>>>>>>> ") );
//
//                ws.sendBinary(bb,false);

                // https://datatracker.ietf.org/doc/html/rfc6455#section-7.4
                CompletableFuture<WebSocket> closeFuture = ws.sendClose(WebSocket.NORMAL_CLOSURE, "ok");

                System.out.println("Close client has been sent.");

                // closeFuture.get();

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        t1.start();



        for(;;){}


    }


}
