package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.marketplace.customer.Customer;
import dk.ku.di.dms.vms.marketplace.product.Product;
import dk.ku.di.dms.vms.marketplace.stock.StockItem;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;
import java.util.logging.Logger;

public class AbstractWorkflowTest {

    protected static final int batchWindowInterval = 3000;

    protected static final Logger logger = Logger.getLogger(AbstractWorkflowTest.class.getCanonicalName());

    protected final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    protected static final Function<String, HttpRequest> httpRequestProductSupplier = str -> HttpRequest.newBuilder( URI.create( "http://localhost:8001/product" ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.ofString( str ))
            .build();

    protected static final Function<String, HttpRequest> httpRequestStockSupplier = str -> HttpRequest.newBuilder( URI.create( "http://localhost:8002/stock" ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.ofString( str ))
            .build();

    protected static final Function<String, HttpRequest> httpRequestCustomerSupplier = str -> HttpRequest.newBuilder( URI.create( "http://localhost:8006/customer" ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.ofString( str ))
            .build();

    protected static final int MAX_ITEMS = 10;
    protected static final int MAX_CUSTOMERS = 10;

    protected void ingestDataIntoProductVms() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        String str1;
        for(int i = 1; i <= MAX_ITEMS; i++){
            str1 = new Product( 1, i, "test", "test", "test", "test", 1.0f, 1.0f,  "test", "test" ).toString();
            HttpRequest prodReq = httpRequestProductSupplier.apply(str1);
            client.send(prodReq, HttpResponse.BodyHandlers.ofString());
        }
    }

    protected void ingestDataIntoStockVms() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        String str2;
        for(int i = 1; i <= MAX_ITEMS; i++){
            str2 = new StockItem( 1, i, 100, 0, 0, 0,  "test", "test" ).toString();
            HttpRequest stockReq = httpRequestStockSupplier.apply(str2);
            client.send(stockReq, HttpResponse.BodyHandlers.ofString());
        }
    }

    protected void ingestDataIntoCustomerVms() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        String str2;
        for(int i = 1; i <= MAX_CUSTOMERS; i++){
            str2 = new Customer( i, "test", "test", "test", "test",
                    "test", "test", "test", "test", "test",
                    "test", "test", "test", "CREDIT_CARD",
                    0, 0, 0, "test" ).toString();
            HttpRequest stockReq = httpRequestCustomerSupplier.apply(str2);
            client.send(stockReq, HttpResponse.BodyHandlers.ofString());
        }
    }

}
