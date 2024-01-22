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
import java.util.*;
import java.util.function.Function;

import static java.lang.Thread.sleep;

public class CheckoutWorkflow extends AbstractWorkflowTest {


    private static final Function<Integer,CustomerCheckout> customerCheckoutFunction = customerId -> new CustomerCheckout(
            customerId, "test", "test", "test", "test","test",
            "test", "test","test","test","test",
            "test", "test", "test", 1,"1"
    );

    @Test
    public void testFullCheckout() throws Exception {

        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);
        dk.ku.di.dms.vms.marketplace.payment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.shipment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.customer.Main.main(null);

        this.ingestDataIntoProductVms();
        this.ingestDataIntoStockVms();
        this.ingestDataIntoCustomerVms();

        Coordinator coordinator = loadCoordinator();
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        Map<String, VmsIdentifier> connectedVMSs;
        do{
            sleep(2000);
            connectedVMSs = coordinator.getConnectedVMSs();
        } while (connectedVMSs.size() < 5);

        Thread thread = new Thread(new CheckoutProducer());
        thread.start();

        sleep(batchWindowInterval * 5);

        assert coordinator.getCurrentBatchOffset() == 2;

        assert coordinator.getBatchOffsetPendingCommit() == 2;

        assert coordinator.getTid() == 11;
    }

    private static Random random = new Random();

    private class CheckoutProducer implements Runnable {
        @Override
        public void run() {
            logger.info("[CheckoutProducer] Starting...");
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );
            int val = 1;
            while(val <= 10) {

                // reserve stock
                ReserveStock reserveStockEvent = new ReserveStock(
                        new Date(), customerCheckoutFunction.apply( random.nextInt(MAX_CUSTOMERS+1) ),
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
            logger.info("[CheckoutProducer] Going to bed definitely... ");
        }
    }


    private Coordinator loadCoordinator() throws IOException {
        ServerIdentifier serverIdentifier = new ServerIdentifier( "localhost", 8080 );

        Map<Integer, ServerIdentifier> serverMap = new HashMap<>(2);
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

        Map<Integer, NetworkAddress> VMSs = new HashMap<>(3);
        NetworkAddress productAddress = new NetworkAddress("localhost", 8081);
        VMSs.put(productAddress.hashCode(), productAddress);
        NetworkAddress stockAddress = new NetworkAddress("localhost", 8082);
        VMSs.put(stockAddress.hashCode(), stockAddress);
        NetworkAddress orderAddress = new NetworkAddress("localhost", 8083);
        VMSs.put(orderAddress.hashCode(), orderAddress);
        NetworkAddress paymentAddress = new NetworkAddress("localhost", 8084);
        VMSs.put(paymentAddress.hashCode(), paymentAddress);
        NetworkAddress shipmentAddress = new NetworkAddress("localhost", 8085);
        VMSs.put(shipmentAddress.hashCode(), shipmentAddress);
        NetworkAddress customerAddress = new NetworkAddress("localhost", 8086);
        VMSs.put(customerAddress.hashCode(), customerAddress);

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
