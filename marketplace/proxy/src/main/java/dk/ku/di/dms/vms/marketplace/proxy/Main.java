package dk.ku.di.dms.vms.marketplace.proxy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.Utils;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static java.lang.Thread.sleep;

/**
 * The proxy builds on top of the coordinator module.
 * Via the proxy, we configure the transactional DAGs and
 * other important configuration properties of the system.
 */
public final class Main {

    private static final int STARTING_TID = 1;
    private static final int STARTING_BATCH_ID = 1;

    private static final BlockingQueue<TransactionInput> TRANSACTION_INPUTS = new LinkedBlockingDeque<>();

    public static void main(String[] ignoredArgs) throws IOException, InterruptedException {
        // read properties
        Properties properties = Utils.loadProperties();

        Coordinator coordinator = loadCoordinator(properties);

        String driverUrl = properties.getProperty("driver_url");

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        int maxSleep = 3;
        do {
            sleep(5000);
            if(coordinator.getConnectedVMSs().size() == 1) break;
            maxSleep--;
        } while (maxSleep > 0);

        if(coordinator.getConnectedVMSs().size() < 3) throw new RuntimeException("Proxy: VMSs did not connect to coordinator on time");

        System.out.println("Proxy: All starter VMS has connected to the coordinator \nProxy: Initializing now the HTTP Server for receiving transaction inputs");

        int http_port = Integer.parseInt( properties.getProperty("http_port") );

        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", http_port), 0);
        httpServer.createContext("/", new ProxyHttpHandler());
        httpServer.start();

        var signalQueue = coordinator.getBatchSignalQueue();
        long initTid = STARTING_TID;
        try (HttpClient httpClient = HttpClient.newHttpClient()) {
            System.out.println("Proxy: Polling for new batch completion signal started");
            for(;;) {
                long lastTid = signalQueue.take();
                System.out.println("Proxy: New batch completion signal received. Last TID executed: "+lastTid);
                // upon a batch completion, send result to driver
                try {
                    HttpRequest httpReq = HttpRequest.newBuilder()
                            .uri(URI.create(driverUrl))
                            .header("Content-Type", "application/text")
                            .version(HttpClient.Version.HTTP_2)
                            .header("keep-alive", "true")
                            .POST(HttpRequest.BodyPublishers.ofString(initTid+"-"+lastTid))
                            .build();
                    httpClient.sendAsync(httpReq, HttpResponse.BodyHandlers.discarding());
                    initTid = lastTid + 1;
                } catch (Exception e) {
                    System.out.println("Proxy: Error while sending HTTP request: \n" + e.getStackTrace()[0]);
                }
            }
        }
    }

    private static Coordinator loadCoordinator(Properties properties) throws IOException {

        int tcpPort = Integer.parseInt( properties.getProperty("tcp_port") );
        ServerNode serverIdentifier = new ServerNode( "localhost", tcpPort );

        Map<Integer, ServerNode> serverMap = new HashMap<>(10);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG updatePriceDag =  TransactionBootstrap.name("update_price")
                .input( "a", "product", "update_price" )
                .terminal("b", "product", "a")
                .build();

        TransactionDAG updateProductDag =  TransactionBootstrap.name("update_product")
                .input( "a", "product", "update_product" )
                .terminal("b", "stock", "a")
                .build();

        TransactionDAG checkoutDag =  TransactionBootstrap.name("customer_checkout")
                .input( "a", "cart", "customer_checkout" )
                .input( "b", "stock", "reserve_stock" )
                .terminal("c", "order", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(updatePriceDag.name, updatePriceDag);
        transactionMap.put(updateProductDag.name, updateProductDag);
        transactionMap.put(checkoutDag.name, checkoutDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        String productHost = properties.getProperty("product_host");
        String stockHost = properties.getProperty("stock_host");
        String orderHost = properties.getProperty("order_host");
        String cartHost = properties.getProperty("cart_host");
        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, Constants.PRODUCT_VMS_PORT);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", stockHost, Constants.STOCK_VMS_PORT);
        IdentifiableNode orderAddress = new IdentifiableNode("order", orderHost, Constants.ORDER_VMS_PORT);
        IdentifiableNode cartAddress = new IdentifiableNode("cart", cartHost, Constants.CART_VMS_PORT);

        Map<Integer, IdentifiableNode> starterVMSs = new HashMap<>(10);
        starterVMSs.put(productAddress.hashCode(), productAddress);
        starterVMSs.put(stockAddress.hashCode(), stockAddress);
        starterVMSs.put(orderAddress.hashCode(), orderAddress);
        starterVMSs.put(cartAddress.hashCode(), cartAddress);

        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        long batchSendRate = Long.parseLong( properties.getProperty("batch_send_rate") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );

        return Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions()
                        .withBatchWindow(batchSendRate)
                        .withGroupThreadPoolSize(groupPoolSize)
                        .withNetworkBufferSize(networkBufferSize),
                STARTING_BATCH_ID,
                STARTING_TID,
                TRANSACTION_INPUTS,
                serdes
        );
    }

    private static class ProxyHttpHandler implements HttpHandler {

        public ProxyHttpHandler(){ }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String payload = new String( exchange.getRequestBody().readAllBytes() );

            // TODO paths for cart (checkout), shipment (delivery)
            String[] uriSplit = exchange.getRequestURI().toString().split("/");

            switch(uriSplit[1]){
                case "cart": {
                    if(exchange.getRequestMethod().equalsIgnoreCase("POST")){
                        TransactionInput.Event eventPayload = new TransactionInput.Event("customer_checkout", payload);
                        TransactionInput txInput = new TransactionInput("customer_checkout", eventPayload);
                        TRANSACTION_INPUTS.add(txInput);
                    } else {
                        System.out.println("Proxy: Unsupported cart HTTP method: " + exchange.getRequestMethod());
                    }
                    break;
                }
                case "product" : {
                    switch (exchange.getRequestMethod()) {
                        // price update
                        case "PATCH": {
                            TransactionInput.Event eventPayload = new TransactionInput.Event("update_price", payload);
                            TransactionInput txInput = new TransactionInput("update_price", eventPayload);
                            TRANSACTION_INPUTS.add(txInput);
                            break;
                        }
                        // product update
                        case "PUT": {
                            TransactionInput.Event eventPayload = new TransactionInput.Event("update_product", payload);
                            TransactionInput txInput = new TransactionInput("update_product", eventPayload);
                            TRANSACTION_INPUTS.add(txInput);
                            break;
                        }
                    }
                }
                case "shipment" : {
                    break;
                }
                default: {
                    OutputStream outputStream = exchange.getResponseBody();
                    exchange.sendResponseHeaders(500, 0);
                    outputStream.flush();
                    outputStream.close();
                    return;
                }
            }

            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(200, 0);
            outputStream.flush();
            outputStream.close();
        }

    }

}