package dk.ku.di.dms.vms.marketplace.proxy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.VmsIdentifier;
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
 * TODO list:
 *  (i) Stream transaction marks via websocket
 *  (ii) Transaction marks must be sent to coordinator
 */
public final class Main {

    private static final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        // read properties
        Properties properties = Utils.loadProperties();

        Coordinator coordinator = loadCoordinator(properties);

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        int maxSleep = 3;
        do {
            sleep(5000);
            if(coordinator.getConnectedVMSs().size() == 1) break;
            maxSleep--;
        } while (maxSleep > 0);

        if(coordinator.getConnectedVMSs().size() < 1) throw new RuntimeException("VMSs did not connect to coordinator on time");

        System.out.println("All starter VMS has connected to the coordinator");

        System.out.println("Initializing now the HTTP Server for receiving transaction inputs");

        int http_port = Integer.parseInt( properties.getProperty("http_port") );

        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", http_port), 0);
        httpServer.createContext("/", new ProxyHttpHandler());
        httpServer.start();

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
                .terminal("b", "product", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(updatePriceDag.name, updatePriceDag);
        transactionMap.put(updateProductDag.name, updateProductDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        String productHost = properties.getProperty("product_host");
        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, Constants.PRODUCT_VMS_PORT);

        Map<Integer, IdentifiableNode> starterVMSs = new HashMap<>(10);
        starterVMSs.put(productAddress.hashCode(), productAddress);

        long batchSendRate = Long.parseLong( properties.getProperty("batch_send_rate") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );

        return Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(batchSendRate).withGroupThreadPoolSize(groupPoolSize),
                1,
                1,
                parsedTransactionRequests,
                serdes
        );
    }

    private static class ProxyHttpHandler implements HttpHandler {

        public ProxyHttpHandler(){ }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String str = new String( exchange.getRequestBody().readAllBytes() );

            // TODO paths for cart (checkout), product (update product and price update), shipment (delivery)

            // response
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(200, 0);
            outputStream.flush();
            outputStream.close();
        }

    }

}