package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.VmsIdentifier;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.product.UpdateProductEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.sleep;

public class ProductStockWorkflowTest extends AbstractWorkflowTest {

    @Test
    public void testLargeBatchWithTwoVMSs() throws Exception {

        dk.ku.di.dms.vms.marketplace.product.Main.main(null);
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);

        this.ingestDataIntoProductVms();
        this.insertItemsInStockVms();

        // initialize coordinator
        Coordinator coordinator = loadCoordinator();

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        Map<String, VmsIdentifier> connectedVMSs;
        do{
            sleep(5000);
            connectedVMSs = coordinator.getConnectedVMSs();
        } while (connectedVMSs.size() < 2);

        Thread thread = new Thread(new Producer());
        thread.start();

        sleep(BATCH_WINDOW_INTERVAL * 3);

        assert coordinator.getBatchOffsetPendingCommit() == 2;
        assert coordinator.getTid() == 10;
        assert coordinator.getCurrentBatchOffset() == 2;
    }

    private Coordinator loadCoordinator() throws IOException {
        ServerIdentifier serverIdentifier = new ServerIdentifier( "localhost", 8080 );

        Map<Integer, ServerIdentifier> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG updatePriceDag =  TransactionBootstrap.name("update_price")
                .input( "a", "product", "update_price" )
                .terminal("b", "product", "a")
                .build();

        TransactionDAG updateProductDag =  TransactionBootstrap.name("update_product")
                .input( "a", "product", "update_product" )
                .terminal("b", "stock", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(updatePriceDag.name, updatePriceDag);
        transactionMap.put(updateProductDag.name, updateProductDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        NetworkAddress productAddress = new NetworkAddress("localhost", 8081);
        NetworkAddress stockAddress = new NetworkAddress("localhost", 8082);

        Map<Integer, NetworkAddress> VMSs = new HashMap<>(2);
        VMSs.put(productAddress.hashCode(), productAddress);
        VMSs.put(stockAddress.hashCode(), stockAddress);

        return Coordinator.build(
                serverMap,
                VMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(BATCH_WINDOW_INTERVAL),
                1,
                1,
                parsedTransactionRequests,
                serdes
        );
    }

    private class Producer implements Runnable {
        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(val < 10) {
                UpdateProductEvent updateProductEvent = new UpdateProductEvent(
                        1,1,"test","test","test","test",10.0F,10.0F,"test",String.valueOf(val)
                );

                String payload = serdes.serialize(updateProductEvent, UpdateProductEvent.class);

                TransactionInput.Event eventPayload = new TransactionInput.Event("update_product", payload);

                TransactionInput txInput = new TransactionInput("update_product", eventPayload);

                logger.info("[Producer] Adding "+val);

                parsedTransactionRequests.add(txInput);

                val++;
            }
            logger.info("Producer going to bed definitely... ");
        }
    }

}
