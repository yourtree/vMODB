package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.VmsIdentifier;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.marketplace.product.Product;
import dk.ku.di.dms.vms.marketplace.product.UpdateProductEvent;
import dk.ku.di.dms.vms.marketplace.stock.Stock;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;

public class WorkflowTest {

    protected static final Logger logger = Logger.getLogger("WorkflowTest");

    private final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    private static final Function<String, HttpRequest> supplier = str -> HttpRequest.newBuilder( URI.create( "http://localhost:8001/product" ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.ofString( str ))
            .build();

    private static final int MAX_ITEMS = 10;

    private void ingestDataIntoVMSs() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        String str1;
        String str2;
        for(int i = 1; i <= MAX_ITEMS; i++){
            str1 = new Product( 1, i, "test", "test", "test", "test", 1.0f, 1.0f,  "test", "test" ).toString();
            HttpRequest prodReq = supplier.apply(str1);
            client.send(prodReq, HttpResponse.BodyHandlers.ofString());

            str2 = new Stock( 1, i, 100, 0, 0, 0,  "test", "test" ).toString();
            HttpRequest stockReq = supplier.apply(str2);
            client.send(stockReq, HttpResponse.BodyHandlers.ofString());
        }
    }

    @Test
    public void testLargeBatchWithTwoVMSs() throws Exception {

//        dk.ku.di.dms.vms.marketplace.product.Main.main(null);

        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);

        // ingestDataIntoVMSs();

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

        assert true;

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
        // VMSs.put(productAddress.hashCode(), productAddress);
        VMSs.put(stockAddress.hashCode(), stockAddress);

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
