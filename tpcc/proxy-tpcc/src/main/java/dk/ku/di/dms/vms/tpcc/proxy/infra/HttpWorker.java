package dk.ku.di.dms.vms.tpcc.proxy.infra;

import java.net.http.HttpClient;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

public abstract class HttpWorker {

    protected static final String CONTENT_TYPE = "Content-Type";
    protected static final String CONTENT_TYPE_VAL = "application/json";

    protected static final ConcurrentLinkedQueue<HttpClient> CLIENT_POOL = new ConcurrentLinkedQueue<>();

    protected static final Supplier<HttpClient> HTTP_CLIENT_SUPPLIER = () -> {
        if (!CLIENT_POOL.isEmpty()) {
            HttpClient client = CLIENT_POOL.poll();
            if (client != null) return client;
        }
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    };

}
