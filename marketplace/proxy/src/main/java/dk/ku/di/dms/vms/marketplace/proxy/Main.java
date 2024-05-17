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
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
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

        var starterVMSs = coordinator.getStarterVMSs();
        int starterSize = starterVMSs.size();
        do {
            sleep(500);
            if(coordinator.getConnectedVMSs().size() == starterSize) break;
            System.out.println("Proxy: Waiting for all starter VMSs to come online. Sleeping for "+500+" ms...");
        } while (true);

        System.out.println("Proxy: All starter VMSs have connected to the coordinator \nProxy: Initializing the HTTP Server to receive transaction inputs...");

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

    private static Map<String, TransactionDAG> buildTransactionDAGs(){
        Map<String, TransactionDAG> transactionMap = new HashMap<>();

        TransactionDAG updatePriceDag = TransactionBootstrap.name(UPDATE_PRICE)
                .input( "a", "product", UPDATE_PRICE )
                .terminal("b", "cart", "a")
                .build();
        transactionMap.put(updatePriceDag.name, updatePriceDag);

        TransactionDAG updateProductDag = TransactionBootstrap.name(UPDATE_PRODUCT)
                .input( "a", "product", UPDATE_PRODUCT )
//                .terminal("b", "stock", "a")
//                .terminal("c", "cart", "a")
                // omit below if you want to skip batch commit info
                .terminal("b", "product", "a")
                .build();
        transactionMap.put(updateProductDag.name, updateProductDag);

        TransactionDAG updateDeliveryDag = TransactionBootstrap.name(UPDATE_DELIVERY)
                .input("a", "shipment", UPDATE_DELIVERY)
                .terminal("b", "order", "a")
                .build();
        transactionMap.put(updateDeliveryDag.name, updateDeliveryDag);

        TransactionDAG checkoutDag = TransactionBootstrap.name(CUSTOMER_CHECKOUT)
                .input("a", "cart", CUSTOMER_CHECKOUT)
                .internal("b", "stock", RESERVE_STOCK, "a")
                .internal("c", "order", STOCK_CONFIRMED, "b")
                .internal("d", "payment", INVOICE_ISSUED, "c")
                .terminal("e", "shipment",  "d" )
                .build();
        transactionMap.put(checkoutDag.name, checkoutDag);

        return transactionMap;
    }

    private static Coordinator loadCoordinator(Properties properties) throws IOException {

        int tcpPort = Integer.parseInt( properties.getProperty("tcp_port") );
        ServerNode serverIdentifier = new ServerNode( "localhost", tcpPort );

        Map<Integer, ServerNode> serverMap = new HashMap<>();
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        Map<String, TransactionDAG> transactionMap = buildTransactionDAGs();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<Integer, IdentifiableNode> starterVMSs;

        String transactionsRaw = properties.getProperty("transactions");
        String[] transactions = transactionsRaw.split(";");
        if(Arrays.stream(transactions).anyMatch(p->p.contentEquals("checkout"))) {
            starterVMSs = buildStarterVMSsFull(properties);
        } else {
            if(transactions.length == 1) {
                starterVMSs = buildStarterVMS(properties);
            } else {
                starterVMSs = buildStarterVMSsBasic(properties);
            }
        }

        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        long batchSendRate = Long.parseLong( properties.getProperty("batch_send_rate") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );

        int definiteBufferSize = networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize;
        System.out.println("Buffer size: "+definiteBufferSize);

        return Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions()
                        .withBatchWindow(batchSendRate)
                        .withGroupThreadPoolSize(groupPoolSize)
                        .withNetworkBufferSize(definiteBufferSize),
                STARTING_BATCH_ID,
                STARTING_TID,
                TRANSACTION_INPUTS,
                serdes
        );
    }

    private static Map<Integer, IdentifiableNode> buildStarterVMS(Properties properties){

        String productHost = properties.getProperty("product_host");

        if(productHost == null) throw new RuntimeException("Product host is null");

        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, Constants.PRODUCT_VMS_PORT);

        Map<Integer, IdentifiableNode> starterVMSs = new HashMap<>();
        starterVMSs.put(productAddress.hashCode(), productAddress);
        return starterVMSs;
    }

    private static Map<Integer, IdentifiableNode> buildStarterVMSsBasic(Properties properties){
        String cartHost = properties.getProperty("cart_host");
        String productHost = properties.getProperty("product_host");
        String stockHost = properties.getProperty("stock_host");

        if(productHost == null) throw new RuntimeException("Product host is null");
        if(cartHost == null) throw new RuntimeException("Cart host is null");
        if(stockHost == null) throw new RuntimeException("Stock host is null");

        IdentifiableNode cartAddress = new IdentifiableNode("cart", cartHost, Constants.CART_VMS_PORT);
        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, Constants.PRODUCT_VMS_PORT);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", stockHost, Constants.STOCK_VMS_PORT);

        Map<Integer, IdentifiableNode> starterVMSs = new HashMap<>();
        starterVMSs.put(cartAddress.hashCode(), cartAddress);
        starterVMSs.put(productAddress.hashCode(), productAddress);
        starterVMSs.put(stockAddress.hashCode(), stockAddress);
        return starterVMSs;
    }

    private static Map<Integer, IdentifiableNode> buildStarterVMSsFull(Properties properties) {
        String cartHost = properties.getProperty("cart_host");
        String productHost = properties.getProperty("product_host");
        String stockHost = properties.getProperty("stock_host");
        String orderHost = properties.getProperty("order_host");
        String paymentHost = properties.getProperty("payment_host");
        String shipmentHost = properties.getProperty("shipment_host");

        if(cartHost == null) throw new RuntimeException("Cart host is null");
        if(productHost == null) throw new RuntimeException("Product host is null");
        if(stockHost == null) throw new RuntimeException("Stock host is null");
        if(orderHost == null) throw new RuntimeException("Order host is null");
        if(paymentHost == null) throw new RuntimeException("Payment host is null");
        if(shipmentHost == null) throw new RuntimeException("Shipment host is null");

        IdentifiableNode cartAddress = new IdentifiableNode("cart", cartHost, Constants.CART_VMS_PORT);
        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, Constants.PRODUCT_VMS_PORT);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", stockHost, Constants.STOCK_VMS_PORT);
        IdentifiableNode orderAddress = new IdentifiableNode("order", orderHost, Constants.ORDER_VMS_PORT);
        IdentifiableNode paymentAddress = new IdentifiableNode("payment", paymentHost, PAYMENT_VMS_PORT);
        IdentifiableNode shipmentAddress = new IdentifiableNode("shipment", shipmentHost, SHIPMENT_VMS_PORT);

        Map<Integer, IdentifiableNode> starterVMSs = new HashMap<>(10);
        starterVMSs.put(cartAddress.hashCode(), cartAddress);
        starterVMSs.put(productAddress.hashCode(), productAddress);
        starterVMSs.put(stockAddress.hashCode(), stockAddress);
        starterVMSs.put(orderAddress.hashCode(), orderAddress);
        starterVMSs.put(paymentAddress.hashCode(), paymentAddress);
        starterVMSs.put(shipmentAddress.hashCode(), shipmentAddress);
        return starterVMSs;
    }

    private static class ProxyHttpHandler implements HttpHandler {

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        public ProxyHttpHandler(){ }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String payload = new String( exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);

            // System.out.println("Proxy: Received input \n"+payload);

            String[] uriSplit = exchange.getRequestURI().toString().split("/");
            switch(uriSplit[1]){
                case "cart": {
                    if(exchange.getRequestMethod().equalsIgnoreCase("POST")){
                        TransactionInput.Event eventPayload = new TransactionInput.Event(CUSTOMER_CHECKOUT, payload);
                        TransactionInput txInput = new TransactionInput(CUSTOMER_CHECKOUT, eventPayload);
                        TRANSACTION_INPUTS.add(txInput);
                        break;
                    }
                    reportError("cart", exchange);
                    return;
                }
                case "product" : {
                    switch (exchange.getRequestMethod()) {
                        // price update
                        case "PATCH": {
                            TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRICE, payload);
                            TransactionInput txInput = new TransactionInput(UPDATE_PRICE, eventPayload);
                            TRANSACTION_INPUTS.add(txInput);
                            break;
                        }
                        // product update
                        case "PUT": {
                            TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRODUCT, payload);
                            TransactionInput txInput = new TransactionInput(UPDATE_PRODUCT, eventPayload);
                            TRANSACTION_INPUTS.add(txInput);
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
                    if(exchange.getRequestMethod().equalsIgnoreCase("PATCH")){
                        TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_DELIVERY, payload);
                        TransactionInput txInput = new TransactionInput(UPDATE_DELIVERY, eventPayload);
                        TRANSACTION_INPUTS.add(txInput);
                        break;
                    }
                    reportError("shipment", exchange);
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
            System.out.println("Proxy: Unsupported "+service+" HTTP method: " + exchange.getRequestMethod());
            endExchange(exchange, 500);
        }

        private static void endExchange(HttpExchange exchange, int rCode) throws IOException {
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(rCode, 0);
            outputStream.flush();
            outputStream.close();
        }

    }

}