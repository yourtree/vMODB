package dk.ku.di.dms.vms.tpcc.proxy.ingestion;

import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

public final class DataLoader {

    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_TYPE_VAL = "application/json";

    private static final ConcurrentLinkedQueue<HttpClient> CLIENT_POOL = new ConcurrentLinkedQueue<>();

    private static final Supplier<HttpClient> HTTP_CLIENT_SUPPLIER = () -> {
        if (!CLIENT_POOL.isEmpty()) {
            HttpClient client = CLIENT_POOL.poll();
            if (client != null) return client;
        }
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    };

    public static void load(Map<String, UniqueHashBufferIndex> tableToIndexMap){

        // iterate over tables, for each, create a set of threads to ingest data

        // let's start with warehouse first
        HttpClient httpClient = HTTP_CLIENT_SUPPLIER.get();

        /*
        HttpRequest httpReq = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header(CONTENT_TYPE, CONTENT_TYPE_VAL)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = httpClient.send(httpReq, HttpResponse.BodyHandlers.ofString());
         */
    }

}
