package dk.ku.di.dms.vms.sidecar.websocket;

import com.google.gson.Gson;
import dk.ku.di.dms.vms.web_common.WebSocketUtils;
import dk.ku.di.dms.vms.sidecar.event.EventExample;
import dk.ku.di.dms.vms.sidecar.server.AsyncVMSServer;
import org.junit.Test;

import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static java.util.logging.Logger.getLogger;

public class WebSocketServerTest {

    private static final Logger logger = getLogger(WebSocketServerTest.class.getName());

    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    @Test
    public void test() {

        // create a thread...
        AsyncVMSServer vmsServer = new AsyncVMSServer();
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

    @Test
    public void testAsynchronousClient() throws IOException, ExecutionException, InterruptedException {

        AsyncVMSServer vmsServer = new AsyncVMSServer();
        Thread t0 = new Thread(vmsServer);
        t0.start();

        AsynchronousSocketChannel socketChannel = AsynchronousSocketChannel.open();
        InetSocketAddress addr = new InetSocketAddress("localhost", 80);

        socketChannel.connect( addr );

        String handshakeRequest = "GET / HTTP/1.1\r\n"
                + "Connection: Upgrade\r\n"
                + "Content-Length: 0\r\n"
                + "Upgrade: websocket\r\n"
                + "User-Agent: Java-http-client/18\r\n"
                + "Sec-WebSocket-Key: " + createNonce() + "\r\n"
                + "Sec-WebSocket-Version: 13\r\n"
                + "Host:127.0.0.1\r\n\r\n";

        byte[] byteToSend = handshakeRequest.getBytes("UTF-8");
        ByteBuffer bb = ByteBuffer.wrap( byteToSend );

        socketChannel.write(bb);

        bb.clear();
        Future<Integer> ft = socketChannel.read( bb );
        ft.get();

        String response = new String( bb.array() );

        logger.info(response);

        EventExample event = new EventExample(1,1,1);
        String json = new Gson().toJson( event );
        // https://stackoverflow.com/questions/3736058/java-object-to-byte-and-byte-to-object-converter-for-tokyo-cabinet

        ByteBuffer bb1 = ByteBuffer.wrap( json.getBytes(StandardCharsets.UTF_8) );

        socketChannel.write( bb1 );

        socketChannel.close();

    }

    @Test
    public void testCustomWebSocketClient() throws ExecutionException, InterruptedException, IOException {

        AsyncVMSServer vmsServer = new AsyncVMSServer();
        Thread t0 = new Thread(vmsServer);
        t0.start();

        Socket socket = new Socket(Proxy.NO_PROXY);
        try {
            socket.setTcpNoDelay(false);
            socket.setReuseAddress(false);

            InetSocketAddress addr = new InetSocketAddress("localhost", 80);

            socket.connect( addr );

        } catch (IOException e) {
            e.printStackTrace();
        }

        if (socket.isConnected()) {
            logger.info("Socket connected!");
        }

        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .executor(executor)
                .build();

        URI httpURI = URI.create("http://127.0.0.1:80");

        // disallowed: "connection", "content-length", "expect", "host", "upgrade"

        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(httpURI)
                .version(HttpClient.Version.HTTP_1_1)
                .GET()
                .header("Sec-WebSocket-Version", "13")
                .header("Sec-WebSocket-Key",createNonce())
                // .method("CONNECT", HttpRequest.BodyPublishers.noBody())
                //.header("Connection", "Upgrade")
                //.header("Upgrade","websocket")
                .header("User-Agent", "VMS")
                //.header("Host", "127.0.0.1")
                //.header("Content-Length", "0")
                .build();


        // String handshakeRequest = httpRequest.toString();
        String handshakeRequest = "GET / HTTP/1.1\r\n"
                + "Connection: Upgrade\r\n"
                + "Content-Length: 0\r\n"
                + "Upgrade: websocket\r\n"
                + "User-Agent: Java-http-client/18\r\n"
                + "Sec-WebSocket-Key: " + createNonce() + "\r\n"
                + "Sec-WebSocket-Version: 13\r\n"
                + "Host:127.0.0.1\r\n\r\n";

        //CompletableFuture<HttpResponse<String>> responseCf = httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());

        //HttpResponse<String> response = responseCf.get();

//        int c = response.statusCode();
//
//        if(c != 101){

            try {
                // ByteBuffer bb1 = ByteBuffer.wrap( WebSocketUtils.StringOperations.encode(handshakeRequest) );
                byte[] byteToSend = handshakeRequest.getBytes("UTF-8");
                socket.getOutputStream().write(byteToSend, 0, byteToSend.length);
                socket.getOutputStream().flush();
                // socket.getOutputStream();
            } catch (IOException e) {
                e.printStackTrace();
            }
//        }

        //byte[] b = new byte[EVENT_PAYLOAD_SIZE];
        //int len = socket.getInputStream().read(b);
        String response = new Scanner(socket.getInputStream(),"UTF-8").useDelimiter("\\r\\n\\r\\n").next();

        // now trying to send some data
        EventExample event = new EventExample(1,1,1);
        String json = new Gson().toJson( event ) + "\r\n\r\n";
        // https://stackoverflow.com/questions/3736058/java-object-to-byte-and-byte-to-object-converter-for-tokyo-cabinet

        byte[] b =  json.getBytes(StandardCharsets.UTF_8);
        // ByteBuffer bb2 = ByteBuffer.wrap( serializedEvent );
        socket.getOutputStream().write(b, 0, b.length);
        socket.getOutputStream().flush();
//        assert c == 101;

        while(true) {}

        // assert response != null;
        // throw WebSocketHandshakeException

    }

    private static String createNonce() {
        byte[] bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }


}
