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
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static java.lang.Thread.sleep;

/**
 * TODO list:
 *  (i) Stream transaction marks via websocket
 *  (ii) Transaction marks must be sent to coordinator
 */
public final class Main {

    private static final String configFile = "app.properties";

    private static final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        // read properties
        Properties properties = new Properties();
        try {

            if (Files.exists(Paths.get(configFile))) {
                System.out.println("Loading external config file: " + configFile);
                properties.load(new FileInputStream(configFile));
            } else {
                System.out.println("Loading internal config file: app.properties");
                properties.load(Main.class.getClassLoader().getResourceAsStream(configFile));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Coordinator coordinator = loadCoordinator(properties);
        Map<String, VmsIdentifier> connectedVMSs;
        int maxSleep = 3;
        do {
            sleep(5000);
            connectedVMSs = coordinator.getConnectedVMSs();
            if(connectedVMSs.size() == 2) break;
            maxSleep--;
        } while (maxSleep > 0);

        if(coordinator.getConnectedVMSs().size() < 2) throw new RuntimeException("VMSs did not connect to coordinator on time");

        int http_port = Integer.parseInt( properties.getProperty("http_port") );

        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", http_port), 0);
        httpServer.createContext("/", new ProxyHttpHandler());
        httpServer.start();

    }

    private static Coordinator loadCoordinator(Properties properties) throws IOException {

        int port = Integer.parseInt( properties.getProperty("port") );
        ServerNode serverIdentifier = new ServerNode( "localhost", port );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
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

        IdentifiableNode productAddress = new IdentifiableNode("product", "localhost", Constants.PRODUCT_VMS_PORT);

        Map<Integer, IdentifiableNode> VMSs = new HashMap<>(2);
        VMSs.put(productAddress.hashCode(), productAddress);

        long batchWindow = Long.parseLong( properties.getProperty("batch_window") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );

        return Coordinator.build(
                serverMap,
                VMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(batchWindow).withGroupThreadPoolSize(groupPoolSize),
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