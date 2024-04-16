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
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import static java.lang.Thread.sleep;

public sealed class CheckoutWorkflowTest extends AbstractWorkflowTest permits UpdateShipmentWorkflowTest {

    private static final Function<Integer,CustomerCheckout> customerCheckoutFunction = customerId -> new CustomerCheckout(
            customerId, "test", "test", "test", "test","test",
            "test", "test","test","test","test",
            "test", "test", "test", 1,"1"
    );

    private void initVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);
        dk.ku.di.dms.vms.marketplace.payment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.shipment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.customer.Main.main(null);

        this.insertItemsInStockVms();
        this.insertCustomersInCustomerVms();
    }

    @Test
    public void testCustomerCheckout() throws Exception {
        this.initVMSs();

        Coordinator coordinator = this.loadCoordinator();
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        this.triggerCheckoutWorkflow(coordinator);

        sleep(BATCH_WINDOW_INTERVAL * 5);

        assert coordinator.getCurrentBatchOffset() == 2;
        assert coordinator.getBatchOffsetPendingCommit() == 2;
        assert coordinator.getTid() == 11;
    }

    protected void triggerCheckoutWorkflow(Coordinator coordinator) throws Exception {
        Map<String, VmsIdentifier> connectedVMSs;
        int numStarterVMSs = coordinator.getStarterVMSs().size();
        do{
            sleep(2000);
            connectedVMSs = coordinator.getConnectedVMSs();
        } while (connectedVMSs.size() < numStarterVMSs);

        new CustomerCheckoutProducer().run();
    }

    private static final Random random = new Random();

    private class CustomerCheckoutProducer implements Runnable {

        private final String name = CustomerCheckoutProducer.class.getSimpleName();

        @Override
        public void run() {
            logger.info("["+name+"] Starting...");
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();
            int val = 1;
            while(val <= 10) {

                // reserve stock
                ReserveStock reserveStockEvent = new ReserveStock(
                        new Date(), customerCheckoutFunction.apply( random.nextInt(1,MAX_CUSTOMERS+1) ),
                        List.of(
                                new CartItem(val,1,"test",
                                        1.0f, 1.0f, 1, 1.0f, "1")
                        ),
                        String.valueOf(val)
                );
                String payload_ = serdes.serialize(reserveStockEvent, ReserveStock.class);
                TransactionInput.Event eventPayload_ = new TransactionInput.Event("reserve_stock", payload_);
                TransactionInput txInput_ = new TransactionInput("customer_checkout", eventPayload_);
                logger.info("["+name+"] New reserve stock event with version: "+val);
                parsedTransactionRequests.add(txInput_);

                val++;
            }
            logger.info("["+name+"] Going to bed definitely...");
        }
    }

    /**  // Another way to define the DAG
         TransactionDAG checkoutDag =  TransactionBootstrap.name("customer_checkout")
         .input( "a", "stock", "reserve_stock" )
         .internal("b", "order", "stock_confirmed", "a")
         .internal("c", "seller", "invoice_issued", "b")
         .internal("c", "payment", "invoice_issued", "b")
         .internal("c", "seller", "payment_confirmed", "c")
         .internal( "e", "shipment",  "c" )
         .terminal("c", "seller", "shipment_notification", "e")
         .terminal( "d", "customer", "c" )
         .build();
     */
    private Coordinator loadCoordinator() throws IOException {
        ServerNode serverIdentifier = new ServerNode( "localhost", 8080 );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG checkoutDag =  TransactionBootstrap.name("customer_checkout")
                .input( "a", "stock", "reserve_stock" )
                .internal("b", "order", "stock_confirmed", "a")
                .internal("c", "payment", "invoice_issued", "b")
                .terminal( "d", "customer", "c" )
                .terminal( "e", "shipment",  "c" )
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(checkoutDag.name, checkoutDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<Integer, IdentifiableNode> starterVMSs = new HashMap<>(10);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", "localhost", 8082);
        starterVMSs.put(stockAddress.hashCode(), stockAddress);
        IdentifiableNode orderAddress = new IdentifiableNode("order", "localhost", 8083);
        starterVMSs.put(orderAddress.hashCode(), orderAddress);
        IdentifiableNode paymentAddress = new IdentifiableNode("payment", "localhost", 8084);
        starterVMSs.put(paymentAddress.hashCode(), paymentAddress);
        IdentifiableNode shipmentAddress = new IdentifiableNode("shipment", "localhost", 8085);
        starterVMSs.put(shipmentAddress.hashCode(), shipmentAddress);
        IdentifiableNode customerAddress = new IdentifiableNode("customer", "localhost", 8086);
        starterVMSs.put(customerAddress.hashCode(), customerAddress);

        return Coordinator.build(
                serverMap,
                starterVMSs,
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
