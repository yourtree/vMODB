package dk.ku.di.dms.vms.sidecar;

import dk.ku.di.dms.vms.sidecar.server.VMSServer;
import org.junit.Test;

import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;
import static java.util.logging.Logger.getLogger;

public class WebSocketServerTest {

    private static final Logger logger = getLogger(WebSocketServerTest.class.getName());

    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    @Test
    public void test() throws InterruptedException, ExecutionException {

        // create a thread...



        Thread t0 = new Thread( () -> {
            try {

                VMSServer vmsServer = new VMSServer();

//                ServerSocket serverSocket = vmsServer.run();
                // ServerSocketChannel channel = vmsServer.setup(socket);

                // serverSocket.close();

                // ServerSocketChannel channel = vmsServer.setup( serverSocket );

//                while(true){
//                    // channel.accept();
//                    Socket socket = serverSocket.accept();
//
//                    System.out.println("New client message");
//
//                    socket.getInputStream();

//                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        t0.start();

        CountDownLatch latch = new CountDownLatch(1);

//        WebSocket.

        // WebSocket.Listener listener = new WebSocket.Listener() {};

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


                ByteBuffer bb = ByteBuffer.wrap( NetworkUtils.encode(" Sending from client SOMETHING>>>>>>>> ") );

                CompletableFuture<WebSocket> msgCf = ws.sendBinary(bb,false);

                WebSocket ws1 = msgCf.get();

                System.out.println("Message sent to server");

                System.out.println("Sending another message to server...");

                bb = ByteBuffer.wrap( NetworkUtils.encode(" Sending from client SOMETHING2>>>>>>>> ") );

                ws.sendBinary(bb,false);

            } catch (InterruptedException | ExecutionException | IOException e) {
                e.printStackTrace();
            }
        });

        t1.start();

//        CompletableFuture<WebSocket> cf = client.newWebSocketBuilder()//.connectTimeout(Duration.ofSeconds(10))
//                .buildAsync(URI.create("ws://127.0.0.1:80"), listener);
//
//        WebSocket ws = cf.get();

//        WebSocket webSocket = client.newWebSocketBuilder()
//                                    .buildAsync(URI.create("ws://127.0.0.1:80"), listener)
//                                    .join();

//        logger.info("WebSocket created");

        // sleep(800);

        //WebSocket wsC = ws.join();

        // wsC.sendText("TESSSSSYE",false);



        // latch.await();

        for(;;){}


//        webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "ok")
//                .thenRun(() -> logger.info("Sent close"))
//                .join();

        // assert(true);

    }


}
