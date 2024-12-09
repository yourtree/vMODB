package dk.ku.di.dms.vms.tpcc.proxy.dataload;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.function.Function;

import static java.lang.System.Logger.Level.INFO;

public final class DataLoadUtils {

    private static final System.Logger LOGGER = System.getLogger(DataLoadUtils.class.getName());

    @SuppressWarnings("rawtypes")
    public static Map<String, QueueTableIterator> mapTablesFromDisk(Map<String, UniqueHashBufferIndex> tableToIndexMap,
                                                                    Map<String, EntityHandler> entityHandlerMap) {
        LOGGER.log(INFO, "Loading tables in memory starting...");
        long init = System.currentTimeMillis();
        Map<String, QueueTableIterator> tableInputMap = new HashMap<>();
        try {
            for(var idx : tableToIndexMap.entrySet()){
                tableInputMap.put(idx.getKey(), new QueueTableIterator(idx.getValue(), entityHandlerMap.get(idx.getKey())));
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            long end = System.currentTimeMillis();
            LOGGER.log(INFO, "Loading tables in memory finished in "+(end-init)+"ms");
        }
        return tableInputMap;
    }

    /**
     * In case the services have been restarted, the cached connections won't work anymore
     * Calling this method is a conservative way to avoid errors on ingesting again in the same experiment session
     */
    private static void releaseAllConnections(){
        for(var entries : IngestionWorker.CONNECTION_POOL.values()){
            for(var conn : entries){
                conn.close();
            }
        }
        IngestionWorker.CONNECTION_POOL.clear();
    }

    public static void ingestData(Map<String, QueueTableIterator> tableInputMap) {
        releaseAllConnections();
        int numCpus = Runtime.getRuntime().availableProcessors();
        ExecutorService threadPool = Executors.newFixedThreadPool(numCpus);
        BlockingQueue<Future<Void>> completionQueue = new ArrayBlockingQueue<>(numCpus);
        CompletionService<Void> service = new ExecutorCompletionService<>(threadPool, completionQueue);
        LOGGER.log(INFO, "Table ingestion starting...");
        long init = System.currentTimeMillis();
        for (int i = 0; i < numCpus; i++) {
            service.submit(new IngestionWorker(tableInputMap), null);
        }
        try {
            for (int i = 0; i < numCpus; i++) {
                completionQueue.poll(5, TimeUnit.MINUTES);
            }
        } catch(InterruptedException e){
            threadPool.shutdownNow();
            e.printStackTrace(System.err);
        } finally{
            long end = System.currentTimeMillis();
            LOGGER.log(INFO, "Table ingestion finished in " + (end - init) + "ms");
        }
    }

    private static class IngestionWorker implements Runnable {

        private static final Properties PROPERTIES = ConfigUtils.loadProperties();

        private static final Map<String, ConcurrentLinkedDeque<MinimalHttpClient>> CONNECTION_POOL = new ConcurrentHashMap<>();

        private static final Function<String, MinimalHttpClient> HTTP_CLIENT_SUPPLIER = (table) -> {
            String service = TPCcConstants.TABLE_TO_VMS_MAP.get(table);
            var clientPool = CONNECTION_POOL.computeIfAbsent(service, (ignored)-> new ConcurrentLinkedDeque<>());
            if (!clientPool.isEmpty()) {
                MinimalHttpClient client = clientPool.poll();
                if (client != null) return client;
            }
            String host = PROPERTIES.getProperty(service + "_host");
            int port = TPCcConstants.VMS_TO_PORT_MAP.get(service);
            try {
                return new MinimalHttpClient(host, port);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        private static void returnConnection(String table, MinimalHttpClient client){
            // return to pool for reuse
            String service = TPCcConstants.TABLE_TO_VMS_MAP.get(table);
            CONNECTION_POOL.get(service).add(client);
        }

        private final Map<String, QueueTableIterator> tableInputMap;

        private IngestionWorker(Map<String, QueueTableIterator> tableInputMap) {
            this.tableInputMap = tableInputMap;
        }

        @Override
        public void run() {
            try {
                for(var table : this.tableInputMap.entrySet()) {
                    MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply(table.getKey());
                    var queue = table.getValue();
                    String entity;
                    int count = 0;
                    LOGGER.log(INFO, "Thread "+Thread.currentThread().threadId()+" starting with table "+table.getKey());
                    while ((entity = queue.poll()) != null) {
                        client.sendRequest("POST", entity, table.getKey());
                        count++;
                    }
                    LOGGER.log(INFO, "Thread "+Thread.currentThread().threadId()+" finished with table "+table.getKey()+": "+count+" records sent.");
                    returnConnection(table.getKey(), client);
                }
            } catch (Exception e){
                e.printStackTrace(System.err);
            }
        }
    }

}
