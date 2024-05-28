package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

/**
 * This class tests the full checkout workflow
 * That is, all VMSs and events part of checkout
 */
public sealed class CheckoutWorkflowTest extends AbstractWorkflowTest permits UpdateShipmentWorkflowTest {

    private void initVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.cart.Main.main(null);
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);
        dk.ku.di.dms.vms.marketplace.payment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.shipment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.customer.Main.main(null);
        dk.ku.di.dms.vms.marketplace.seller.Main.main(null);

        this.insertItemsInStockVms();
        this.insertCustomersInCustomerVms();
    }

    @Test
    public void testCheckout() throws Exception {
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
        int numStarterVMSs = coordinator.getStarterVMSs().size();
        do{
            sleep(2000);
        } while (coordinator.getConnectedVMSs().size() < numStarterVMSs);

        new ReserveStockProducer(coordinator).run();
    }

    private static final Random random = new Random();

    private static class ReserveStockProducer implements Runnable {

        private final String name = ReserveStockProducer.class.getSimpleName();

        Coordinator coordinator;

        public ReserveStockProducer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {
            logger.log(INFO, "["+this.name+"] Starting...");
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
                logger.log(INFO, "["+this.name+"] New reserve stock event with version: "+val);
                coordinator.queueTransactionInput(txInput_);

                val++;
            }
            logger.log(INFO, "["+this.name+"] Going to bed definitely...");
        }
    }

    private Coordinator loadCoordinator() throws IOException {
        ServerNode serverIdentifier = new ServerNode( "localhost", 8080 );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG checkoutDag =  TransactionBootstrap.name(CUSTOMER_CHECKOUT)
                .input( "a", "cart", CUSTOMER_CHECKOUT)
                .internal( "b", "stock", RESERVE_STOCK, "a")
                .internal("c", "order", STOCK_CONFIRMED, "b")
                .internal("d", "payment", INVOICE_ISSUED, "c")
                .terminal("e", "seller", "c")
                .terminal( "f", "customer", "d" )
                .terminal( "g", "shipment",  "d" )
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(checkoutDag.name, checkoutDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<Integer, IdentifiableNode> starterVMSs = getStaterVMSs();

        return Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(1000),
                1,
                1,
                serdes
        );
    }

    private static Map<Integer, IdentifiableNode> getStaterVMSs() {
        Map<Integer, IdentifiableNode> starterVMSs = new HashMap<>();
        IdentifiableNode cartAddress = new IdentifiableNode("cart", "localhost", CART_VMS_PORT);
        starterVMSs.put(cartAddress.hashCode(), cartAddress);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", "localhost", STOCK_VMS_PORT);
        starterVMSs.put(stockAddress.hashCode(), stockAddress);
        IdentifiableNode orderAddress = new IdentifiableNode("order", "localhost", ORDER_VMS_PORT);
        starterVMSs.put(orderAddress.hashCode(), orderAddress);
        IdentifiableNode paymentAddress = new IdentifiableNode("payment", "localhost", PAYMENT_VMS_PORT);
        starterVMSs.put(paymentAddress.hashCode(), paymentAddress);
        IdentifiableNode sellerAddress = new IdentifiableNode("seller", "localhost", SELLER_VMS_PORT);
        starterVMSs.put(sellerAddress.hashCode(), sellerAddress);
        IdentifiableNode customerAddress = new IdentifiableNode("customer", "localhost", CUSTOMER_VMS_PORT);
        starterVMSs.put(customerAddress.hashCode(), customerAddress);
        IdentifiableNode shipmentAddress = new IdentifiableNode("shipment", "localhost", SHIPMENT_VMS_PORT);
        starterVMSs.put(shipmentAddress.hashCode(), shipmentAddress);
        return starterVMSs;
    }

}
