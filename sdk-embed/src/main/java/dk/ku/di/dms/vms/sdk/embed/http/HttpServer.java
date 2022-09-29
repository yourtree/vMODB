package dk.ku.di.dms.vms.sdk.embed.http;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 *
 */
public class HttpServer {

    public static void start(String hostname, int port) throws IOException {

        System.setProperty("sun.net.httpserver.nodelay","true");

        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.
                create(new InetSocketAddress(hostname, port), 0);

        HttpContext cxt = server.createContext("/load", new MyHttpHandler());

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
