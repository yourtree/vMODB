package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.VmsIdentifier;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
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

import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

/**
 *
 */
public final class StockOrderWorkflowTest extends AbstractWorkflowTest {

    private static final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    private static final CustomerCheckout customerCheckout = new CustomerCheckout();

    @Test
    public void testTopologyWithTwoVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);

        this.insertItemsInStockVms();

        Coordinator coordinator = loadCoordinator();

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        Map<String, VmsIdentifier> connectedVMSs;
        do{
            sleep(5000);
            connectedVMSs = coordinator.getConnectedVMSs();
        } while (connectedVMSs.size() < 2);

        Thread thread = new Thread(new InputProducer());
        thread.start();

        sleep(BATCH_WINDOW_INTERVAL * 3);

        assert coordinator.getCurrentBatchOffset() == 2;
        assert coordinator.getBatchOffsetPendingCommit() == 2;
        assert coordinator.getTid() == 21;
    }

    private static class InputProducer implements Runnable {
        @Override
        public void run() {
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );
            int val = 1;
            while(val < 21) {

                // reserve stock
                ReserveStock reserveStockEvent = new ReserveStock(
                        new Date(), customerCheckout,
                        List.of( new CartItem(1,1,"test",1.0f, 1.0f, 1, 1.0f, "test") ),
                        String.valueOf(val)
                );
                String payload_ = serdes.serialize(reserveStockEvent, ReserveStock.class);
                TransactionInput.Event eventPayload_ = new TransactionInput.Event("reserve_stock", payload_);
                TransactionInput txInput_ = new TransactionInput("customer_checkout", eventPayload_);
                logger.log(INFO, "[CheckoutProducer] New reserve stock event with version: "+val);
                parsedTransactionRequests.add(txInput_);

                val++;
            }
            logger.log(INFO, "InputProducer going to bed definitely... ");
        }
    }

    private Coordinator loadCoordinator() throws IOException {
        ServerNode serverIdentifier = new ServerNode( "localhost", 8080 );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG checkoutDag =  TransactionBootstrap.name("customer_checkout")
                .input( "a", "stock", "reserve_stock" )
                .terminal("b", "order", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(checkoutDag.name, checkoutDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        Map<Integer, IdentifiableNode> VMSs = new HashMap<>(3);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", "localhost", 8082);
        VMSs.put(stockAddress.hashCode(), stockAddress);
        IdentifiableNode orderAddress = new IdentifiableNode("order", "localhost", 8083);
        VMSs.put(orderAddress.hashCode(), orderAddress);

        return Coordinator.build(
                serverMap,
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
