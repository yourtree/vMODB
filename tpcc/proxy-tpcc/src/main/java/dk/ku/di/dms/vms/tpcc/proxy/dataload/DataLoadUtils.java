package dk.ku.di.dms.vms.tpcc.proxy.dataload;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static java.lang.System.Logger.Level.INFO;

public final class DataLoadUtils {

    private static final System.Logger LOGGER = System.getLogger(DataLoadUtils.class.getName());

    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();

    @SuppressWarnings("rawtypes")
    public static Map<String, Queue<String>> loadTablesInMemory(Map<String, UniqueHashBufferIndex> tableToIndexMap,
                                             Map<String, EntityHandler> entityHandlerMap) {
        LOGGER.log(INFO, "Loading tables in memory starting...");
        long init = System.currentTimeMillis();

        // iterate over tables, for each, create a set of threads to ingest data
        List<Future<Queue<String>>> futures = new ArrayList<>();
        for(var idx : tableToIndexMap.entrySet()){
            // for testing only
            // if(!idx.getKey().contentEquals("stock")) continue;
            LOGGER.log(INFO, "Submitting data load worker for table "+idx.getKey());
            futures.add(
                    THREAD_POOL.submit(new DataLoadWorker(idx.getValue(), entityHandlerMap.get(idx.getKey())))
                    );
        }

        Map<String, Queue<String>> tableInputMap = new HashMap<>();
        int i = 0;
        LOGGER.log(INFO, "Waiting for tables to load...");
        try {
            for(var idx : tableToIndexMap.entrySet()){
                var queue = futures.get(i).get();
                LOGGER.log(INFO, "Table "+idx.getKey()+" finished loading.");
                if(queue != null){
                    tableInputMap.put(idx.getKey(), queue);
                }
                i++;
            }
        } catch (InterruptedException | ExecutionException e){
            throw new RuntimeException(e);
        } finally {
            long end = System.currentTimeMillis();
            LOGGER.log(INFO, "Loading tables in memory finished in "+(end-init)+"ms");
        }

        return tableInputMap;
    }

    @SuppressWarnings("rawtypes")
    private static class DataLoadWorker implements Callable<Queue<String>> {

        private final UniqueHashBufferIndex index;
        private final EntityHandler entityHandler;

        private DataLoadWorker(UniqueHashBufferIndex index, EntityHandler entityHandler) {
            this.index = index;
            this.entityHandler = entityHandler;
        }

        @Override
        public Queue<String> call() {
            Queue<String> queue = new ConcurrentLinkedQueue<>();
            try {
                IRecordIterator<IKey> iterator = this.index.iterator();
                while (iterator.hasNext()) {
                    Object[] record = this.index.record(iterator);
                    var entity = this.entityHandler.parseObjectIntoEntity(record);
                    queue.add(entity.toString());
                    iterator.next();
                }
            } catch (Exception e){
                e.printStackTrace(System.err);
            }
            return queue;
        }
    }

    public static void ingestData(Map<String, Queue<String>> tableInputMap) {
        int numCpus = Runtime.getRuntime().availableProcessors();
        ExecutorService threadPool = Executors.newFixedThreadPool(numCpus);
        BlockingQueue<Future<Void>> completionQueue = new ArrayBlockingQueue<>(numCpus);
        CompletionService<Void> service = new ExecutorCompletionService<>(threadPool, completionQueue);
        LOGGER.log(INFO, "Ingesting tables starting...");
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
            LOGGER.log(INFO, "Ingesting tables finished in " + (end - init) + "ms");
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

        private final Map<String, Queue<String>> tableInputMap;

        private IngestionWorker(Map<String, Queue<String>> tableInputMap) {
            this.tableInputMap = tableInputMap;
        }

        @Override
        public void run() {
            try {
                for(var table : this.tableInputMap.entrySet()) {
                    MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply(table.getKey());
                    var queue = table.getValue();
                    String entity;
                    while ((entity = queue.poll()) != null) {
                        client.sendRequest(entity, table.getKey());
                    }
                    LOGGER.log(INFO, "Thread "+Thread.currentThread().threadId()+" finished with table "+table.getKey());
                    returnConnection(table.getKey(), client);
                }
            } catch (Exception e){
                e.printStackTrace(System.err);
            }
        }
    }

}
