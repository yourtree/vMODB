package dk.ku.di.dms.vms.coordinator.server.http.jdk;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

/**
 * <a href="https://dzone.com/articles/simple-http-server-in-java">...</a>
 * It would be nice to compare both implementations.
 */
public class EdgeHttpServerBuilder {

    public static HttpServer start(IVmsSerdesProxy serdesProxy,
                                   BlockingQueue<TransactionInput> parsedTransactionRequests,
                                   Map<String, TransactionDAG> transactionMap) throws IOException {

        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 8001), 10);

        // HttpContext newTxCtx =
        server.createContext("/newTx", new TransactionInputHandler(serdesProxy, parsedTransactionRequests ));
        // HttpContext newTxDefCtx =
        server.createContext("/newTxDef", new TransactionDefinitionHandler(serdesProxy, transactionMap ));

        server.setExecutor(Executors.newSingleThreadExecutor());

        server.start();

        return server;

    }

    public static class TransactionDefinitionHandler implements HttpHandler {

        private final IVmsSerdesProxy serdesProxy;

        private final Map<String, TransactionDAG> transactionMap;

        public TransactionDefinitionHandler(IVmsSerdesProxy serdesProxy, Map<String, TransactionDAG> transactionMap) {
            this.serdesProxy = serdesProxy;
            this.transactionMap = transactionMap;
        }

        @Override
        public void handle(HttpExchange exchange) {
            InputStream is = exchange.getRequestBody();
            try(is) {
                String json = new String(is.readAllBytes());
                TransactionDAG dag = this.serdesProxy.deserialize(json, TransactionDAG.class);
                this.transactionMap.put(dag.name, dag);
                exchange.sendResponseHeaders(200, 0);
            } catch(Exception ignored) {}
        }
    }

    public static class TransactionInputHandler implements HttpHandler {

        private final IVmsSerdesProxy serdesProxy;

        private final BlockingQueue<TransactionInput> parsedTransactionRequests;

        public TransactionInputHandler(IVmsSerdesProxy serdesProxy, BlockingQueue<TransactionInput> parsedTransactionRequests){
            this.serdesProxy = serdesProxy;
            this.parsedTransactionRequests = parsedTransactionRequests;
        }

        @Override
        public void handle(HttpExchange exchange) {
            // https://www.codeproject.com/Tips/1040097/Create-a-Simple-Web-Server-in-Java-HTTP-Server
            // InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
            // BufferedReader br = new BufferedReader(isr);

            // buffer.clear();
            // Channels.newChannel(exchange.getRequestBody()).read(buffer);

            // must also check whether the event is correct, that is, contains all events
            InputStream is = exchange.getRequestBody();
            try (is) {
                String json = new String(is.readAllBytes());
                TransactionInput transactionInput = this.serdesProxy.deserialize(json, TransactionInput.class);
                // order by name, since we guarantee the topology input events are ordered by name
                transactionInput.events.sort(Comparator.comparing(o -> o.name));
                this.parsedTransactionRequests.add(transactionInput);
                exchange.sendResponseHeaders(200, 0);
            } catch (Exception ignored) { }
        }

    }

}
