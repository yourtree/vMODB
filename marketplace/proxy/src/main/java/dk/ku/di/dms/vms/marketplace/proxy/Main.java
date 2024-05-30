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
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.System.Logger.Level.*;
import static java.lang.Thread.sleep;

/**
 * The proxy builds on top of the coordinator module.
 * Via the proxy, we configure the transactional DAGs and
 * other important configuration properties of the system.
 */
@SuppressWarnings("InfiniteLoopStatement")
public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    private static final int NUM_CPUS = Runtime.getRuntime().availableProcessors();

    private static final int STARTING_TID = 1;
    private static final int STARTING_BATCH_ID = 1;

    private static final int SLEEP = 1000;

    // private static final BlockingQueue<TransactionInput> TRANSACTION_INPUTS = new LinkedBlockingDeque<>();

    public static void main(String[] ignoredArgs) throws IOException, InterruptedException {
        Properties properties = ConfigUtils.loadProperties();
        Coordinator coordinator = loadCoordinator(properties);
        initHttpServer(properties, coordinator);
        ackBatchCompletionLoop(properties, coordinator.getBatchSignalQueue());
    }

    @SuppressWarnings("BusyWait")
    private static void waitForAllStarterVMSs(Coordinator coordinator) throws InterruptedException {
        var starterVMSs = coordinator.getStarterVMSs();
        int starterSize = starterVMSs.size();
        do {
            sleep(SLEEP);
            if(coordinator.getConnectedVMSs().size() == starterSize) break;
            LOGGER.log(INFO, "Proxy: Waiting for all starter VMSs to come online. Sleeping for "+SLEEP+" ms...");
        } while (true);

        LOGGER.log(INFO,"Proxy: All starter VMSs have connected to the coordinator \nProxy: Initializing the HTTP Server to receive transaction inputs...");
    }

    private static void initHttpServer(Properties properties, Coordinator coordinator) throws IOException, InterruptedException {

        waitForAllStarterVMSs(coordinator);

        int http_port = Integer.parseInt( properties.getProperty("http_port") );
        int http_thread_pool_size = Integer.parseInt( properties.getProperty("http_thread_pool_size") );
        int backlog = Integer.parseInt( properties.getProperty("backlog") );

        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", http_port), backlog);
        if(http_thread_pool_size > 0) {
            httpServer.setExecutor(Executors.newFixedThreadPool(http_thread_pool_size));
        } else {
            // do not use cached thread pool here
            httpServer.setExecutor(Executors.newWorkStealingPool(NUM_CPUS));
        }
        httpServer.createContext("/", new ProxyHttpHandler(coordinator));
        httpServer.start();
    }

    private static void ackBatchCompletionLoop(Properties properties, BlockingQueue<Long> signalQueue) throws InterruptedException {

        boolean driverAlive = Boolean.parseBoolean(properties.getProperty("driver_alive"));
        if(!driverAlive) { return; }
        String driverUrl = properties.getProperty("driver_url");

        long initTid = STARTING_TID;
        try (HttpClient httpClient = HttpClient.newHttpClient()) {
            LOGGER.log(INFO,"Proxy: Polling for new batch completion signal started");
            for(;;) {
                long lastTid = signalQueue.take();
                LOGGER.log(DEBUG,"Proxy: New batch completion signal received. Last TID executed: "+lastTid);
                // upon a batch completion, send result to driver
                try {
                    HttpRequest httpReq = HttpRequest.newBuilder()
                            .uri(URI.create(driverUrl))
                            .header("Content-Type", "application/text")
                            .version(HttpClient.Version.HTTP_2)
                            .header("keep-alive", "true")
                            .POST(HttpRequest.BodyPublishers.ofString(initTid+"-"+lastTid))
                            .build();
                    httpClient.send(httpReq, HttpResponse.BodyHandlers.discarding());
                    initTid = lastTid + 1;
                } catch (Exception e) {
                    LOGGER.log(ERROR,"Proxy: Error while sending HTTP request: \n" +
                            (e.getStackTrace().length > 0 ? e.getStackTrace()[0] : e));
                }
            }
        }
    }

    private static class ProxyHttpHandler implements HttpHandler {

        private final Coordinator coordinator;

        public ProxyHttpHandler(Coordinator coordinator){
            this.coordinator = coordinator;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String payload = new String( exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String[] uriSplit = exchange.getRequestURI().toString().split("/");
            switch(uriSplit[1]){
                case "cart": {
                    if(exchange.getRequestMethod().equalsIgnoreCase("POST")){
                        TransactionInput.Event eventPayload = new TransactionInput.Event(CUSTOMER_CHECKOUT, payload);
                        TransactionInput txInput = new TransactionInput(CUSTOMER_CHECKOUT, eventPayload);
                        if(this.coordinator.queueTransactionInput(txInput))
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
                            if(this.coordinator.queueTransactionInput(txInput))
                                break;
                        }
                        // product update
                        case "PUT": {
                            TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRODUCT, payload);
                            TransactionInput txInput = new TransactionInput(UPDATE_PRODUCT, eventPayload);
                            if(this.coordinator.queueTransactionInput(txInput))
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
                        if(this.coordinator.queueTransactionInput(txInput))
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
            LOGGER.log(ERROR,"Proxy: Unsupported "+service+" HTTP method: " + exchange.getRequestMethod());
            endExchange(exchange, 500);
        }

        private static void endExchange(HttpExchange exchange, int rCode) throws IOException {
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(rCode, 0);
            outputStream.flush();
            outputStream.close();
        }
    }

    private static Map<String, TransactionDAG> buildTransactionDAGs(String[] transactionList){
        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        Set<String> transactions = Set.of(transactionList);

        if(transactions.contains(UPDATE_PRICE)) {
            TransactionDAG updatePriceDag = TransactionBootstrap.name(UPDATE_PRICE)
                    .input("a", "product", UPDATE_PRICE)
                    .terminal("b", "cart", "a")
                    .build();
            transactionMap.put(updatePriceDag.name, updatePriceDag);
        }

        if(transactions.contains(UPDATE_PRODUCT)) {
            TransactionDAG updateProductDag = TransactionBootstrap.name(UPDATE_PRODUCT)
                    .input("a", "product", UPDATE_PRODUCT)
                    .terminal("b", "stock", "a")
                    .terminal("c", "cart", "a")
                    // omit below if you want to skip batch commit info
//                .terminal("b", "product", "a")
                    .build();
            transactionMap.put(updateProductDag.name, updateProductDag);
        }

        if(transactions.contains(UPDATE_DELIVERY)) {
            TransactionDAG updateDeliveryDag = TransactionBootstrap.name(UPDATE_DELIVERY)
                    .input("a", "shipment", UPDATE_DELIVERY)
                    .terminal("b", "order", "a")
                    .build();
            transactionMap.put(updateDeliveryDag.name, updateDeliveryDag);
        }

        if(transactions.contains(CUSTOMER_CHECKOUT)) {
            TransactionDAG checkoutDag = TransactionBootstrap.name(CUSTOMER_CHECKOUT)
                    .input("a", "cart", CUSTOMER_CHECKOUT)
                    .internal("b", "stock", RESERVE_STOCK, "a")
                    .internal("c", "order", STOCK_CONFIRMED, "b")
                    .internal("d", "payment", INVOICE_ISSUED, "c")
                    //.terminal("any", "customer", "b")
                    .terminal("e", "shipment", "d")
                    .build();
            transactionMap.put(checkoutDag.name, checkoutDag);
        }

        return transactionMap;
    }

    private static Coordinator loadCoordinator(Properties properties) throws IOException {

        int tcpPort = Integer.parseInt( properties.getProperty("tcp_port") );
        ServerNode serverIdentifier = new ServerNode( "localhost", tcpPort );

        Map<Integer, ServerNode> serverMap = new HashMap<>();
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        String transactionsRaw = properties.getProperty("transactions");
        String[] transactions = transactionsRaw.split(",");
        Map<String, TransactionDAG> transactionMap = buildTransactionDAGs(transactions);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<Integer, IdentifiableNode> starterVMSs;
        if(Arrays.stream(transactions).anyMatch(p->p.contentEquals(CUSTOMER_CHECKOUT))) {
            starterVMSs = buildStarterVMSsFull(properties);
        } else {
            if(transactions.length == 1) {
                starterVMSs = buildStarterVMS(properties);
            } else {
                starterVMSs = buildStarterVMSsBasic(properties);
            }
        }

        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        int osBufferSize = Integer.parseInt( properties.getProperty("os_buffer_size") );
        long batchSendRate = Long.parseLong( properties.getProperty("batch_send_rate") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );
        int networkSendTimeout = Integer.parseInt( properties.getProperty("network_send_timeout") );
        int task_thread_pool_size = Integer.parseInt( properties.getProperty("task_thread_pool_size") );

        int definiteBufferSize = networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize;

        Coordinator coordinator = Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions()
                        .withBatchWindow(batchSendRate)
                        .withGroupThreadPoolSize(groupPoolSize)
                        .withNetworkBufferSize(definiteBufferSize)
                        .withNetworkSendTimeout(networkSendTimeout)
                        .withOsBufferSize(osBufferSize)
                        .withTaskThreadPoolSize(task_thread_pool_size > 0 ? task_thread_pool_size : NUM_CPUS / 2),
                STARTING_BATCH_ID,
                STARTING_TID,
                serdes
        );

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();
        return coordinator;
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

}