package dk.ku.di.dms.vms.tpcc.proxy.dataload;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static java.lang.System.Logger.Level.INFO;

public final class DataLoader {

    private static final System.Logger LOGGER = System.getLogger(DataLoader.class.getName());

    @SuppressWarnings({"rawtypes"})
    public static boolean load(Map<String, UniqueHashBufferIndex> tableToIndexMap,
                            Map<String, EntityHandler> entityHandlerMap, int numWorkers) {
        ExecutorService threadPool = Executors.newFixedThreadPool(numWorkers);
        BlockingQueue<Future<Void>> completionQueue = new ArrayBlockingQueue<>(tableToIndexMap.size());
        CompletionService<Void> service = new ExecutorCompletionService<>(threadPool, completionQueue);

        LOGGER.log(INFO, "Loading tables starting...");
        long init = System.currentTimeMillis();

        // iterate over tables, for each, create a set of threads to ingest data
        for(var idx : tableToIndexMap.entrySet()){
            // for testing only
            if(!idx.getKey().contentEquals("stock")) continue;
            LOGGER.log(INFO, "Submitting ingestion worker for table "+idx.getKey());
            service.submit(new IngestionWorker(idx.getKey(), idx.getValue(), entityHandlerMap.get(idx.getKey())), null);
        }

        LOGGER.log(INFO, "Waiting for tables to load...");
        try {
            for (int i = 0; i < 1
                    //tableToIndexMap.size()
                    ; i++) {
                completionQueue.poll(5, TimeUnit.MINUTES);
            }
        } catch (InterruptedException e){
            threadPool.shutdownNow();
            e.printStackTrace(System.err);
            return false;
        } finally {
            long end = System.currentTimeMillis();
            LOGGER.log(INFO, "Loading tables finished in "+(end-init)+"ms");
        }
        return true;
    }

    @SuppressWarnings({"rawtypes"})
    private static class IngestionWorker implements Runnable {

        private final String table;
        private final UniqueHashBufferIndex index;
        private final EntityHandler entityHandler;

        private IngestionWorker(String table, UniqueHashBufferIndex index, EntityHandler entityHandler) {
            this.table = table;
            this.index = index;
            this.entityHandler = entityHandler;
        }

        @Override
        public void run() {
            try {

                IRecordIterator<IKey> iterator = this.index.iterator();

                String vms = TPCcConstants.TABLE_TO_VMS_MAP.get(this.table);
                int port = TPCcConstants.VMS_TO_PORT_MAP.get(vms);

                Properties properties = ConfigUtils.loadProperties();
                String host = properties.getProperty(vms+"_host");

                String url = "http://"+ host+":"+port+"/"+this.table;

                MinimalHttpClient client = new MinimalHttpClient(url);
                while (iterator.hasNext()) {
                    Object[] record = this.index.record(iterator);
                    var entity = this.entityHandler.parseObjectIntoEntity(record);
                    // it would be nice to collect errors
                    var resp = client.sendRequest(entity.toString());
                    iterator.next();
                }

            } catch (Exception e){
                e.printStackTrace(System.err);
            }
        }
    }

}
