package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.VmsIdentifier;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.entities.CustomerCheckout;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.product.UpdateProductEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;

/**
 * Different transactions in stock VMS interleave
 * Some come from product (update product), others come from the coordinator (reserve stock)
 */
public class ProductStockOrderWorkflowTest extends AbstractWorkflowTest {

    protected static final Logger logger = Logger.getLogger(ProductStockWorkflowTest.class.getCanonicalName());

    private final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    private static final CustomerCheckout customerCheckout = new CustomerCheckout(
         1, "test", "test", "test", "test","test",
            "test", "test","test","test","test",
            "test", "test", "test", 1,"1"
            );

    @Test
    public void testComplexTopologyWithThreeVMSs() throws Exception {

        dk.ku.di.dms.vms.marketplace.product.Main.main(null);

        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);

        dk.ku.di.dms.vms.marketplace.order.Main.main(null);

        ingestDataIntoVMSs();

        Coordinator coordinator = loadCoordinator();

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        Map<String, VmsIdentifier> connectedVMSs;
        do{
            sleep(5000);
            connectedVMSs = coordinator.getConnectedVMSs();
        } while (connectedVMSs.size() < 3);

        Thread thread = new Thread(new InputProducer());
        thread.start();

        sleep(batchWindowInterval * 5);

        assert coordinator.getCurrentBatchOffset() == 2;

        assert coordinator.getBatchOffsetPendingCommit() == 2;

        assert coordinator.getTid() == 21;

    }

    private class InputProducer implements Runnable {
        @Override
        public void run() {
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );
            int val = 1;
            while(val <= 10) {

                // update product
                UpdateProductEvent updateProductEvent = new UpdateProductEvent(
                        1,1,"test","test","test","test",10.0F,10.0F,"test", String.valueOf(val)
                );
                String payload = serdes.serialize(updateProductEvent, UpdateProductEvent.class);
                TransactionInput.Event eventPayload = new TransactionInput.Event("update_product", payload);
                TransactionInput txInput = new TransactionInput("update_product", eventPayload);
                logger.info("[InputProducer] New product version: "+val);
                parsedTransactionRequests.add(txInput);

                // reserve stock
                ReserveStock reserveStockEvent = new ReserveStock(
                        new Date(), customerCheckout,
                        List.of(new CartItem(1,1,"test",1.0f, 1.0f, 1, 1.0f, String.valueOf(val))),
                        String.valueOf(val)
                );
                String payload_ = serdes.serialize(reserveStockEvent, ReserveStock.class);
                TransactionInput.Event eventPayload_ = new TransactionInput.Event("reserve_stock", payload_);
                TransactionInput txInput_ = new TransactionInput("customer_checkout", eventPayload_);
                logger.info("[CheckoutProducer] New reserve stock event with version: "+val);
                parsedTransactionRequests.add(txInput_);

                val++;
            }
            logger.info("InputProducer going to bed definitely... ");
        }
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

        TransactionDAG checkoutDag =  TransactionBootstrap.name("customer_checkout")
                .input( "a", "stock", "reserve_stock" )
                .terminal("b", "order", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(updatePriceDag.name, updatePriceDag);
        transactionMap.put(updateProductDag.name, updateProductDag);
        transactionMap.put(checkoutDag.name, checkoutDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        Map<Integer, NetworkAddress> VMSs = new HashMap<>(3);
        NetworkAddress productAddress = new NetworkAddress("localhost", 8081);
        VMSs.put(productAddress.hashCode(), productAddress);
        NetworkAddress stockAddress = new NetworkAddress("localhost", 8082);
        VMSs.put(stockAddress.hashCode(), stockAddress);
        NetworkAddress orderAddress = new NetworkAddress("localhost", 8083);
        VMSs.put(orderAddress.hashCode(), orderAddress);

        return Coordinator.buildDefault(
                serverMap,
                null,
                VMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(3000),
                1,
                1,
                parsedTransactionRequests,
                serdes
        );
    }

}
