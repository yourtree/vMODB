package dk.ku.di.dms.vms.sidecar.websocket;

import dk.ku.di.dms.vms.web_common.WebSocketUtils;
import dk.ku.di.dms.vms.sidecar.event.EventExample;
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
                    .version(HttpClient.Version.HTTP_2)
                    .executor(executor)
                    .build();

            CompletableFuture<WebSocket> cf = client.newWebSocketBuilder()//.connectTimeout(Duration.ofSeconds(10))
                    .buildAsync(URI.create("ws://127.0.0.1:80"), listener);

//            GET / HTTP/1.1
//            Connection: Upgrade
//            Content-Length: 0
//            Host: 127.0.0.1
//            Upgrade: websocket
//            User-Agent: Java-http-client/18
//            Sec-WebSocket-Key: jjz4Ejq5zGLQ1M5XeUeyGg==
//            Sec-WebSocket-Version: 13

            try {
                WebSocket ws = cf.get();
                logger.info("Web socket client created");

                ByteBuffer bb1 = ByteBuffer.wrap( WebSocketUtils.StringOperations.encode(" Sending from client SOMETHING>>>>>>>> ") );

//                CompletableFuture<WebSocket> msgCf = ws.sendBinary(bb1,true);

                EventExample event = new EventExample(1,1,1);

                // https://stackoverflow.com/questions/3736058/java-object-to-byte-and-byte-to-object-converter-for-tokyo-cabinet

                byte[] serializedEvent = WebSocketUtils.serialize( event );

                ByteBuffer bb2 = ByteBuffer.wrap( serializedEvent );

                ws.sendBinary(bb2,true);
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

            } catch (InterruptedException | ExecutionException | IOException e) {
                e.printStackTrace();
            }
        });

        t1.start();



        for(;;){}


    }


}
