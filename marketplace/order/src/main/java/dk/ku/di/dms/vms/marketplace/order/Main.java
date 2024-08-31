package dk.ku.di.dms.vms.marketplace.order;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.concurrent.ForkJoinPool;

import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                Constants.ORDER_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.order",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        VmsApplication vms = VmsApplication.build(options);
        vms.start();
        initHttpServerJdk(vms);
    }

    private static void initHttpServerJdk(VmsApplication vms) throws IOException {
        // initialize HTTP server for data ingestion
        System.setProperty("sun.net.httpserver.nodelay","true");
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", Constants.ORDER_HTTP_PORT), 0);
        httpServer.createContext("/order", new OrderHttpHandlerJdk(vms));
        httpServer.setExecutor(ForkJoinPool.commonPool());
        httpServer.start();
        LOGGER.log(INFO, "Cart HTTP Server initialized");
    }

    private static class OrderHttpHandlerJdk implements HttpHandler {

        private final VmsApplication vms;

        public OrderHttpHandlerJdk(VmsApplication vms) {
            this.vms = vms;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestMethod().equals("PATCH")) {
                try {
                    vms.getTransactionManager().reset();
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