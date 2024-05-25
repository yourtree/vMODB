package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static dk.ku.di.dms.vms.marketplace.common.Constants.UPDATE_DELIVERY;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

/**
 * This test composes with the checkout workflow.
 * Why? Because it is necessary to have orders. These
 * eventually leads to shipments and associated packages.
 * The packages are then progressively updated
 */
public non-sealed class UpdateShipmentWorkflowTest extends CheckoutWorkflowTest {

    /**
     * Overriding because this test only wants the behavior of checkout workflow, not testing it
     */
    @Override
    public void testCheckout() {
        assert true;
    }

    @Test
    public void testUpdateShipment() throws Exception {

        this.initVMSs();

        Coordinator coordinator = this.loadCoordinator();
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        logger.log(INFO, "Triggering checkout workflow...");
        this.triggerCheckoutWorkflow(coordinator);

        logger.log(INFO, "Waiting batch window interval...");
        sleep(BATCH_WINDOW_INTERVAL * 7);

        logger.log(INFO, "Sending update shipment event...");
        // now send the update shipment event
        new UpdateShipmentProducer(coordinator).run();

        sleep(BATCH_WINDOW_INTERVAL * 3);

        assert coordinator.getCurrentBatchOffset() == 3;
        assert coordinator.getBatchOffsetPendingCommit() == 3;
        assert coordinator.getTid() == 12;
    }

    private static class UpdateShipmentProducer implements Runnable {

        private final String name = UpdateShipmentProducer.class.getSimpleName();

        Coordinator coordinator;

        public UpdateShipmentProducer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {
            logger.log(INFO, "["+name+"] Starting...");
            String instanceId = "1";

            // event name
            TransactionInput.Event eventPayload_ = new TransactionInput.Event(UPDATE_DELIVERY, instanceId);

            // transaction name
            TransactionInput txInput_ = new TransactionInput(UPDATE_DELIVERY, eventPayload_);

            logger.log(INFO, "["+name+"] New update shipment event with version: "+instanceId);
            coordinator.queueTransactionInput(txInput_);

            logger.log(INFO, "["+name+"] Going to bed definitely... ");
        }

    }

    private void initVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);
        dk.ku.di.dms.vms.marketplace.payment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.shipment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.customer.Main.main(null);

        this.insertItemsInStockVms();
        this.insertCustomersInCustomerVms();
    }

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

        TransactionDAG updateShipmentDag =  TransactionBootstrap.name("update_shipment")
                .input( "a", "shipment", "update_shipment" )
                // .terminal("b", "seller", "a")
                .terminal( "c", "customer", "a" )
                .terminal( "d", "order",  "a" )
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(checkoutDag.name, checkoutDag);
        transactionMap.put(updateShipmentDag.name, updateShipmentDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<Integer, IdentifiableNode> starterVMSs = new HashMap<>(10);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", "localhost", 8082);
        starterVMSs.put(stockAddress.hashCode(), stockAddress);
        IdentifiableNode orderAddress = new IdentifiableNode("order", "localhost", 8083);
        starterVMSs.put(orderAddress.hashCode(), orderAddress);
        IdentifiableNode paymentAddress = new IdentifiableNode("payment", "localhost", 8084);
        starterVMSs.put(paymentAddress.hashCode(), paymentAddress);
        IdentifiableNode shipmentAddress = new IdentifiableNode("shipment","localhost", 8085);
        starterVMSs.put(shipmentAddress.hashCode(), shipmentAddress);
        IdentifiableNode customerAddress = new IdentifiableNode("customer", "localhost", 8086);
        starterVMSs.put(customerAddress.hashCode(), customerAddress);
        IdentifiableNode sellerAddress = new IdentifiableNode("seller", "localhost", 8087);
        // starterVMSs.put(sellerAddress.hashCode(), sellerAddress);

        return Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(3000),
                1,
                1,
                serdes
        );
    }

}
