package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
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

public class SellerDashboardTest extends AbstractWorkflowTest {

    @Test
    public void test() throws Exception {
         dk.ku.di.dms.vms.marketplace.seller.Main.main(null);

    }

    private Coordinator loadCoordinator() throws IOException {
        ServerIdentifier serverIdentifier = new ServerIdentifier( "localhost", 8080 );

        Map<Integer, ServerIdentifier> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG simpleDAG =  TransactionBootstrap.name("update_shipment")
                .input( "a", "seller", "invoice_issued" )
                .terminal("b", "seller", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(simpleDAG.name, simpleDAG);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<Integer, NetworkAddress> starterVMSs = new HashMap<>(3);
        NetworkAddress sellerAddress = new NetworkAddress("localhost", 8087);
        starterVMSs.put(sellerAddress.hashCode(), sellerAddress);

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

    protected static final Function<String, HttpRequest> httpRequestSellerSupplier = str -> HttpRequest.newBuilder(
                    URI.create( "http://localhost:8007/seller" ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.ofString( str ))
            .build();

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

}
