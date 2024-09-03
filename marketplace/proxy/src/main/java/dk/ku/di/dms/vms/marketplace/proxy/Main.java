package dk.ku.di.dms.vms.marketplace.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.proxy.http.HttpServerBuilder;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.IOException;
import java.util.*;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

/**
 * The proxy builds on top of the coordinator module.
 * Via the proxy, we configure the transactional DAGs and
 * other important configuration properties of the system.
 */
public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] ignoredArgs) throws IOException, InterruptedException {
        Properties properties = ConfigUtils.loadProperties();
        Coordinator coordinator = loadCoordinator(properties);
        initHttpServer(properties, coordinator);
    }

    private static void initHttpServer(Properties properties, Coordinator coordinator) throws IOException, InterruptedException {
        waitForAllStarterVMSs(coordinator);
        HttpServerBuilder.build(properties, coordinator);
    }

    @SuppressWarnings("BusyWait")
    private static void waitForAllStarterVMSs(Coordinator coordinator) throws InterruptedException {
        final int SLEEP = 1000;
        var starterVMSs = coordinator.getStarterVMSs();
        int starterSize = starterVMSs.size();
        LOGGER.log(INFO, "Proxy: Waiting for all starter VMSs to come online. Sleeping for "+SLEEP+" ms...");
        while (coordinator.getConnectedVMSs().size() < starterSize) {
            sleep(SLEEP);
        }
        LOGGER.log(INFO,"Proxy: All starter VMSs have connected to the coordinator");
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
                    // .internal("e", "seller", "c")
                    // opt to minimize number of votes
                    .internal("e", "seller", INVOICE_ISSUED, "c")
                    .terminal("f", "shipment", "d")
                    .build();
            transactionMap.put(checkoutDag.name, checkoutDag);
        }

        return transactionMap;
    }

    private static Coordinator loadCoordinator(Properties properties) throws IOException {
        final int STARTING_TID = 1;
        final int STARTING_BATCH_ID = 1;

        int tcpPort = Integer.parseInt( properties.getProperty("tcp_port") );
        ServerNode serverIdentifier = new ServerNode( "0.0.0.0", tcpPort );

        Map<Integer, ServerNode> serverMap = new HashMap<>();
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        String transactionsRaw = properties.getProperty("transactions");
        String[] transactions = transactionsRaw.split(",");
        Map<String, TransactionDAG> transactionMap = buildTransactionDAGs(transactions);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<String, IdentifiableNode> starterVMSs;
        if(Arrays.stream(transactions).anyMatch(p->p.contentEquals(CUSTOMER_CHECKOUT))) {
            starterVMSs = buildStarterVMSsFull(properties);
        } else {
            starterVMSs = buildStarterVMSsBasic(properties);
        }

        // network
        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        int osBufferSize = Integer.parseInt( properties.getProperty("os_buffer_size") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );
        int networkSendTimeout = Integer.parseInt( properties.getProperty("network_send_timeout") );
        int definiteBufferSize = networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize;

        // batch generation
        int batchWindow = Integer.parseInt( properties.getProperty("batch_window_ms") );
        int batchMaxTransactions = Integer.parseInt( properties.getProperty("num_max_transactions_batch") );
        int numTransactionWorkers = Integer.parseInt( properties.getProperty("num_transaction_workers") );

        // vms worker config
        int numWorkersPerVms = Integer.parseInt( properties.getProperty("num_vms_workers") );
        int numQueuesVmsWorker = Integer.parseInt( properties.getProperty("num_queues_vms_worker"));
        int maxSleep = Integer.parseInt( properties.getProperty("max_sleep") );

        // logging
        boolean logging = Boolean.parseBoolean( properties.getProperty("logging") );

        Coordinator coordinator = Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions()
                        .withNetworkBufferSize(definiteBufferSize)
                        .withOsBufferSize(osBufferSize)
                        .withNetworkThreadPoolSize(groupPoolSize)
                        .withNetworkSendTimeout(networkSendTimeout)
                        .withBatchWindow(batchWindow)
                        .withMaxTransactionsPerBatch(batchMaxTransactions)
                        .withNumTransactionWorkers(numTransactionWorkers)
                        .withNumWorkersPerVms(numWorkersPerVms)
                        .withNumQueuesVmsWorker(numQueuesVmsWorker)
                        .withMaxVmsWorkerSleep(maxSleep)
                        .withLogging(logging)
                        ,
                STARTING_BATCH_ID,
                STARTING_TID,
                serdes
        );

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();
        return coordinator;
    }

    private static Map<String, IdentifiableNode> buildStarterVMSsBasic(Properties properties){
        String cartHost = properties.getProperty("cart_host");
        String productHost = properties.getProperty("product_host");
        String stockHost = properties.getProperty("stock_host");

        if(productHost == null) throw new RuntimeException("Product host is null");
        if(cartHost == null) throw new RuntimeException("Cart host is null");
        if(stockHost == null) throw new RuntimeException("Stock host is null");

        return getIdentifiableNodeMap(cartHost, productHost, stockHost);
    }

    private static Map<String, IdentifiableNode> getIdentifiableNodeMap(String cartHost, String productHost, String stockHost) {
        IdentifiableNode cartAddress = new IdentifiableNode("cart", cartHost, Constants.CART_VMS_PORT);
        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, Constants.PRODUCT_VMS_PORT);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", stockHost, Constants.STOCK_VMS_PORT);

        Map<String, IdentifiableNode> starterVMSs = new HashMap<>();
        starterVMSs.putIfAbsent(cartAddress.identifier, cartAddress);
        starterVMSs.putIfAbsent(productAddress.identifier, productAddress);
        starterVMSs.putIfAbsent(stockAddress.identifier, stockAddress);
        return starterVMSs;
    }

    private static Map<String, IdentifiableNode> buildStarterVMSsFull(Properties properties) {
        Map<String, IdentifiableNode> starterVMSs = buildStarterVMSsBasic(properties);

        String orderHost = properties.getProperty("order_host");
        String paymentHost = properties.getProperty("payment_host");
        String shipmentHost = properties.getProperty("shipment_host");
        String sellerHost = properties.getProperty("seller_host");

        if(orderHost == null) throw new RuntimeException("Order host is null");
        if(paymentHost == null) throw new RuntimeException("Payment host is null");
        if(shipmentHost == null) throw new RuntimeException("Shipment host is null");
        if(sellerHost == null) throw new RuntimeException("Seller host is null");

        IdentifiableNode orderAddress = new IdentifiableNode("order", orderHost, ORDER_VMS_PORT);
        IdentifiableNode paymentAddress = new IdentifiableNode("payment", paymentHost, PAYMENT_VMS_PORT);
        IdentifiableNode shipmentAddress = new IdentifiableNode("shipment", shipmentHost, SHIPMENT_VMS_PORT);
        IdentifiableNode sellerAddress = new IdentifiableNode("seller", sellerHost, SELLER_VMS_PORT);

        starterVMSs.putIfAbsent(orderAddress.identifier, orderAddress);
        starterVMSs.putIfAbsent(paymentAddress.identifier, paymentAddress);
        starterVMSs.putIfAbsent(shipmentAddress.identifier, shipmentAddress);
        starterVMSs.putIfAbsent(sellerAddress.identifier, sellerAddress);
        return starterVMSs;
    }

}