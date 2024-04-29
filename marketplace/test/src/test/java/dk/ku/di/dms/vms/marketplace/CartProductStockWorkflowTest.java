package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.VmsIdentifier;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.common.Utils;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdatePrice;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdateProduct;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.Thread.sleep;

public final class CartProductStockWorkflowTest extends AbstractWorkflowTest {

    private static final int WAIT_TIME = 5000;

    @Test
    public void testBasicCartProductStockWorkflow() throws Exception {

        dk.ku.di.dms.vms.marketplace.product.Main.main(null);
        dk.ku.di.dms.vms.marketplace.cart.Main.main(null);
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);

        this.ingestDataIntoProductVms();
        this.insertItemsInStockVms();

        // initialize coordinator
        Properties properties = Utils.loadProperties();
        Coordinator coordinator = loadCoordinator(properties);

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        Map<String, VmsIdentifier> connectedVMSs;
        int maxSleep = 3;
        do {
            sleep(WAIT_TIME);
            connectedVMSs = coordinator.getConnectedVMSs();
            if(connectedVMSs.size() == 3) break;
            maxSleep--;
        } while (maxSleep > 0);

        if(coordinator.getConnectedVMSs().size() < 3) throw new RuntimeException("VMSs did not connect to coordinator on time");

        Thread thread = new Thread(new Producer());
        thread.start();

        sleep(BATCH_WINDOW_INTERVAL * 2);

        assert coordinator.getTid() == 21;
        assert coordinator.getBatchOffsetPendingCommit() == 2;
    }

    private Coordinator loadCoordinator(Properties properties) throws IOException {
        int tcpPort = Integer.parseInt( properties.getProperty("tcp_port") );
        ServerNode serverIdentifier = new ServerNode( "localhost", tcpPort );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG updatePriceDag =  TransactionBootstrap.name(UPDATE_PRICE)
                .input("a", "product", UPDATE_PRICE)
                .terminal("b", "cart", "a")
                .build();

        TransactionDAG updateProductDag =  TransactionBootstrap.name(UPDATE_PRODUCT)
                .input( "a", "product", UPDATE_PRODUCT)
                .terminal("b", "stock", "a")
                .terminal("c", "cart", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(updatePriceDag.name, updatePriceDag);
        transactionMap.put(updateProductDag.name, updateProductDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        String productHost = properties.getProperty("product_host");
        String cartHost = properties.getProperty("cart_host");
        String stockHost = properties.getProperty("stock_host");

        Map<Integer, IdentifiableNode> VMSs = getIdentifiableNodeMap(productHost, cartHost, stockHost);

        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        long batchSendRate = Long.parseLong( properties.getProperty("batch_send_rate") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );

        return Coordinator.build(
                serverMap,
                VMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions()
                        .withBatchWindow(batchSendRate)
                        .withGroupThreadPoolSize(groupPoolSize)
                        .withNetworkBufferSize(networkBufferSize),
                1,
                1,
                parsedTransactionRequests,
                serdes
        );
    }

    private static Map<Integer, IdentifiableNode> getIdentifiableNodeMap(String productHost, String cartHost, String stockHost) {
        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, PRODUCT_VMS_PORT);
        IdentifiableNode cartAddress = new IdentifiableNode("cart", cartHost, CART_VMS_PORT);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", stockHost, STOCK_VMS_PORT);

        Map<Integer, IdentifiableNode> VMSs = new HashMap<>();
        VMSs.put(productAddress.hashCode(), productAddress);
        VMSs.put(cartAddress.hashCode(), cartAddress);
        VMSs.put(stockAddress.hashCode(), stockAddress);
        return VMSs;
    }

    private static class Producer implements Runnable {
        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(val <= 10) {
                produceProductUpdate(val, serdes);
                producePriceUpdate(val, serdes);
                val++;
            }
            logger.info("Producer going to bed definitely... ");
        }

        private static void produceProductUpdate(int val, IVmsSerdesProxy serdes) {
            UpdateProduct updateProduct = new UpdateProduct(
                    1,1,"test","test","test","test",10.0F,10.0F,"test", String.valueOf(val)
            );
            String payload = serdes.serialize(updateProduct, UpdateProduct.class);
            TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRODUCT, payload);
            TransactionInput txInput = new TransactionInput(UPDATE_PRODUCT, eventPayload);
            logger.info("[Producer] New product version: "+ val);
            parsedTransactionRequests.add(txInput);
        }

        private static void producePriceUpdate(int val, IVmsSerdesProxy serdes) {
            UpdatePrice priceUpdate = new UpdatePrice(1,1,10.0F, String.valueOf(val));
            String payload = serdes.serialize(priceUpdate, UpdatePrice.class);
            TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRICE, payload);
            TransactionInput txInput = new TransactionInput(UPDATE_PRICE, eventPayload);
            logger.info("[Producer] New product price: "+ val);
            parsedTransactionRequests.add(txInput);
        }
    }

}
