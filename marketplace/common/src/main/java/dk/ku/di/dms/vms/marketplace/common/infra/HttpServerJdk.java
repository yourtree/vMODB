package dk.ku.di.dms.vms.marketplace.common.infra;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.concurrent.ForkJoinPool;

public final class HttpServerJdk {

    public static void init(VmsApplication vms, String context, int port) throws IOException {
        // initialize HTTP server for data ingestion
        System.setProperty("sun.net.httpserver.nodelay","true");
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        httpServer.createContext(context, new HttpHandlerJdk(vms));
        httpServer.setExecutor(ForkJoinPool.commonPool());
        httpServer.start();
    }

    private static class HttpHandlerJdk implements HttpHandler {

        private final VmsApplication vms;

        public HttpHandlerJdk(VmsApplication vms) {
            this.vms = vms;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestMethod().equals("PATCH")) {
                try {
                    this.vms.getTransactionManager().reset();
                    OutputStream outputStream = exchange.getResponseBody();
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                    outputStream.close();
                } catch (Exception e) {
                    returnFailed(exchange);
                }
                return;
            }
            returnFailed(exchange);
        }

        private static void returnFailed(HttpExchange exchange) throws IOException {
            // failed response
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
            outputStream.close();
        }
    }

}
