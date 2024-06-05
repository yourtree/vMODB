package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdatePrice;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

public final class CartProductWorkflowTest extends AbstractWorkflowTest {

    @Test
    public void testBasicCartProductWorkflow() throws Exception {

        dk.ku.di.dms.vms.marketplace.product.Main.main(null);
        dk.ku.di.dms.vms.marketplace.cart.Main.main(null);

        this.ingestDataIntoProductVms();

        // initialize coordinator
        Properties properties = ConfigUtils.loadProperties();
        Coordinator coordinator = loadCoordinator(properties);

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        int maxSleep = 3;
        do {
            sleep(5000);
            if(coordinator.getConnectedVMSs().size() == 2) break;
            maxSleep--;
        } while (maxSleep > 0);

        if(coordinator.getConnectedVMSs().size() < 2) throw new RuntimeException("VMSs did not connect to coordinator on time");

        Thread thread = new Thread(new Producer(coordinator));
        thread.start();

        sleep(BATCH_WINDOW_INTERVAL * 3);

        assert coordinator.getTid() == 11;
    }

    private Coordinator loadCoordinator(Properties properties) throws IOException {
        int tcpPort = Integer.parseInt( properties.getProperty("tcp_port") );
        ServerNode serverIdentifier = new ServerNode( "localhost", tcpPort );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG updatePriceDag =  TransactionBootstrap.name(UPDATE_PRICE)
                .input( "a", "product", UPDATE_PRICE )
                .terminal("b", "cart", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(updatePriceDag.name, updatePriceDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        String productHost = properties.getProperty("product_host");
        String cartHost = properties.getProperty("cart_host");

        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, PRODUCT_VMS_PORT);
        IdentifiableNode cartAddress = new IdentifiableNode("cart", cartHost, CART_VMS_PORT);

        Map<Integer, IdentifiableNode> VMSs = new HashMap<>(2);
        VMSs.put(productAddress.hashCode(), productAddress);
        VMSs.put(cartAddress.hashCode(), cartAddress);

        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        int batchSendRate = Integer.parseInt( properties.getProperty("batch_send_rate") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );

        return Coordinator.build(
                serverMap,
                VMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions()
                        .withBatchWindow(batchSendRate)
                        .withNetworkThreadPoolSize(groupPoolSize)
                        .withNetworkBufferSize(networkBufferSize),
                1,
                1,
                serdes
        );
    }

    private static class Producer implements Runnable {

        Coordinator coordinator;

        public Producer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(val < 10) {
                UpdatePrice priceUpdate = new UpdatePrice(
                        1,1,10.0F, String.valueOf(val)
                );

                String payload = serdes.serialize(priceUpdate, UpdatePrice.class);

                TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRICE, payload);

                TransactionInput txInput = new TransactionInput(UPDATE_PRICE, eventPayload);

                logger.log(INFO, "[Producer] Adding "+val);

                coordinator.queueTransactionInput(txInput);

                val++;
            }
            logger.log(INFO, "Producer going to bed definitely... ");
        }
    }

}
