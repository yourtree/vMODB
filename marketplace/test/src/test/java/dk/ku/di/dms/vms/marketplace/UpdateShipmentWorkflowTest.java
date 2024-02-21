package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.seller.entities.Seller;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.lang.Thread.sleep;

/**
 * This test composes with the checkout workflow.
 * Why? Because it is necessary to have orders. These
 * eventually leads to shipments and associated packages.
 * The packages are then progressively updated
 */
public non-sealed class UpdateShipmentWorkflowTest extends CheckoutWorkflowTest {

    @Override
    public void testCustomerCheckout() {
        assert true;
    }

    @Test
    public void testUpdateShipment() throws Exception {

        this.initVMSs();

        Coordinator coordinator = this.loadCoordinator();
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        logger.info("Triggering checkout workflow...");
        this.triggerCheckoutWorkflow(coordinator);

        logger.info("Waiting batch window interval...");
        sleep(BATCH_WINDOW_INTERVAL * 7);

        logger.info("Sending update shipment event...");
        // now send the update shipment event
        new UpdateShipmentProducer().run();

        sleep(BATCH_WINDOW_INTERVAL * 3);

        //
        assert coordinator.getCurrentBatchOffset() == 3;
        assert coordinator.getBatchOffsetPendingCommit() == 3;
        assert coordinator.getTid() == 12;
    }

    private class UpdateShipmentProducer implements Runnable {

        private final String name = UpdateShipmentProducer.class.getSimpleName();

        @Override
        public void run() {
            logger.info("["+name+"] Starting...");
            String instanceId = "1";

            // event name
            TransactionInput.Event eventPayload_ = new TransactionInput.Event("update_shipment", instanceId);

            // transaction name
            TransactionInput txInput_ = new TransactionInput("update_shipment", eventPayload_);

            logger.info("["+name+"] New update shipment event with version: "+instanceId);
            parsedTransactionRequests.add(txInput_);

            logger.info("["+name+"] Going to bed definitely... ");
        }

    }

    private void initVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);
        dk.ku.di.dms.vms.marketplace.payment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.shipment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.customer.Main.main(null);
        // dk.ku.di.dms.vms.marketplace.seller.Main.main(null);

        this.insertItemsInStockVms();
        this.insertCustomersInCustomerVms();
        // this.insertSellersInSellerVms();
    }

    protected void insertSellersInSellerVms() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        String str;
        for(int i = 1; i <= MAX_SELLERS; i++){
            str = new Seller(i, "test", "test", "test",
                    "test", "test", "test", "test",
                    "test", "test", "test", "test", "test").toString();
            HttpRequest sellerReq = httpRequestSellerSupplier.apply(str);
            client.send(sellerReq, HttpResponse.BodyHandlers.ofString());
        }
    }

    protected static final Function<String, HttpRequest> httpRequestSellerSupplier = str -> HttpRequest.newBuilder(
            URI.create( "http://localhost:8007/seller" ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.ofString( str ))
            .build();

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

        Map<Integer, NetworkAddress> starterVMSs = new HashMap<>(3);
        NetworkAddress stockAddress = new NetworkAddress("localhost", 8082);
        starterVMSs.put(stockAddress.hashCode(), stockAddress);
        NetworkAddress orderAddress = new NetworkAddress("localhost", 8083);
        starterVMSs.put(orderAddress.hashCode(), orderAddress);
        NetworkAddress paymentAddress = new NetworkAddress("localhost", 8084);
        starterVMSs.put(paymentAddress.hashCode(), paymentAddress);
        NetworkAddress shipmentAddress = new NetworkAddress("localhost", 8085);
        starterVMSs.put(shipmentAddress.hashCode(), shipmentAddress);
        NetworkAddress customerAddress = new NetworkAddress("localhost", 8086);
        starterVMSs.put(customerAddress.hashCode(), customerAddress);
        NetworkAddress sellerAddress = new NetworkAddress("localhost", 8087);
        // starterVMSs.put(sellerAddress.hashCode(), sellerAddress);

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
