package dk.ku.di.dms.vms.marketplace.proxy.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.marketplace.proxy.Main;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import static java.lang.System.Logger.Level.ERROR;

final class HttpServerJdk {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void init(Properties properties, Coordinator coordinator) throws IOException {

        int http_port = Integer.parseInt( properties.getProperty("http_port") );
        int http_thread_pool_size = Integer.parseInt( properties.getProperty("http_thread_pool_size") );
        int backlog = Integer.parseInt( properties.getProperty("backlog") );
        String executor = properties.getProperty("executor");

        // System.setProperty("sun.net.httpserver.nodelay","true");

        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", http_port), backlog);
        if(http_thread_pool_size > 0) {
            httpServer.setExecutor(Executors.newFixedThreadPool(http_thread_pool_size));
        } else {
            // do not use cached thread pool here
            if(executor.equalsIgnoreCase("vthread"))
                httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            else if(executor.equalsIgnoreCase("fork")){
                httpServer.setExecutor(ForkJoinPool.commonPool());
            }
        }
        httpServer.createContext("/", new ProxyHttpHandler(coordinator));
        httpServer.start();
    }

    private static class ProxyHttpHandler extends AbstractHttpHandler implements HttpHandler {

        public ProxyHttpHandler(Coordinator coordinator){
            super(coordinator);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String payload = new String( exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String[] uriSplit = exchange.getRequestURI().toString().split("/");
            switch(uriSplit[1]){
                case "cart": {
                    // assert exchange.getRequestMethod().equals("POST");
                    this.submitCustomerCheckout(payload);
                    break;
                }
                case "product" : {
                    switch (exchange.getRequestMethod()) {
                        // price update
                        case "PATCH": {
                            this.submitUpdatePrice(payload);
                            break;
                        }
                        // product update
                        case "PUT": {
                            this.submitUpdateProduct(payload);
                            break;
                        }
                        default: {
                            reportError("product", exchange);
                            return;
                        }
                    }
                    break;
                }
                case "shipment" : {
                    // assert exchange.getRequestMethod().equals("PATCH");
                    this.submitUpdateDelivery(payload);
                    break;
                }
                case "status" : {
                    // assumed to be a get request
                    // assert exchange.getRequestMethod().equals("GET");
                    byte[] b = this.getLastTidBytes();
                    OutputStream outputStream = exchange.getResponseBody();
                    // b.length always equals to 8
                    exchange.sendResponseHeaders(200, Long.BYTES);
                    outputStream.write(b);
                    outputStream.close();
                    return;
                }
                default : {
                    reportError("", exchange);
                    return;
                }
            }
            endExchange(exchange, 200);
        }

        private static void reportError(String service, HttpExchange exchange) throws IOException {
            LOGGER.log(ERROR,"Proxy: Unsupported "+service+" HTTP method: " + exchange.getRequestMethod());
            endExchange(exchange, 500);
        }

        private static void endExchange(HttpExchange exchange, int rCode) throws IOException {
            exchange.sendResponseHeaders(rCode, 0);
            exchange.getResponseBody().close();
            //exchange.close();
        }
    }

}
