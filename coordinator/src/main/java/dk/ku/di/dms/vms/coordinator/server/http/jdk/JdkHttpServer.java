package dk.ku.di.dms.vms.coordinator.server.http.jdk;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 *
 * https://dzone.com/articles/simple-http-server-in-java
 *
 * It would be nice to compare both implementations.
 *
 */
public class JdkHttpServer {

    public static void start() throws IOException {

        System.setProperty("sun.net.httpserver.nodelay","true");

        HttpServer server = HttpServer.
                create(new InetSocketAddress("localhost", 8001), 0);

        HttpContext cxt = server.createContext("/test", new MyHttpHandler());

        server.setExecutor(Executors.newSingleThreadExecutor());

        server.start();

    }

    private static class MyHttpHandler implements HttpHandler {


        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // exchange.getRequestBody().readAllBytes()
        }
    }

}
