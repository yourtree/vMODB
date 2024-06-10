package dk.ku.di.dms.vms.marketplace.proxy;

import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchAlgo;
import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.IVmsWorker;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.TransactionWorker;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

import static java.lang.Thread.sleep;

/**
 * measure the capacity of the transaction worker to generate transaction inputs
 * without having to deploy the full system
 * that allows to verify the scalability of the batch generation multi-thread algorithm
 * ideally should on par with the http server input rate
 */
public final class Main {

    public static void main(String[] ignoredArgs) throws IOException, InterruptedException {
        Properties properties = ConfigUtils.loadProperties();
        int num_transaction_workers = Integer.parseInt( properties.getProperty("num_transaction_workers") );
        int num_max_transactions_batch = Integer.parseInt( properties.getProperty("num_max_transactions_batch") );
        int max_time = Integer.parseInt( properties.getProperty("max_time") );

        System.out.println("Experiment config: \n"+
                " Num transaction workers "+ (num_transaction_workers)+"\n"+
                " Num max transaction batch "+ (num_max_transactions_batch)+"\n"+
                " Max time "+ (max_time)+"\n"
        );

        int NUM_QUEUED_TRANSACTIONS = 4000000;

        List<ConcurrentLinkedDeque<TransactionInput>> txInputQueues = new ArrayList<>(num_transaction_workers);

        // fill each queue with proper number of transaction inputs
        for (int i = 1; i <= num_transaction_workers; i++) {
           var inputQueue = new ConcurrentLinkedDeque<TransactionInput>();
           txInputQueues.add(inputQueue);
            System.out.println("Generaitng "+NUM_QUEUED_TRANSACTIONS+" transactions for worker # "+i);
           for(int j = 1; j <= NUM_QUEUED_TRANSACTIONS; j++) {
               inputQueue.add(new TransactionInput("update_product",
                       new TransactionInput.Event("update_product", String.valueOf(i))));

           }
        }

        Deque<Object> coordinatorQueue = new ConcurrentLinkedDeque<>();
        List<TransactionWorker> workers = setupTransactionWorkers(num_transaction_workers, num_max_transactions_batch,
                coordinatorQueue, txInputQueues);

        var threadFactory = Thread.ofPlatform().factory();
        for (TransactionWorker worker : workers) {
            threadFactory.newThread(worker).start();
        }

        // 10 seconds
        sleep(max_time);

        for (TransactionWorker worker : workers) {
            worker.stop();
        }

        // cannot poll last because the batches from different workers may have interleaved
        Object obj;
        long lastTid = 0;
        while ((obj = coordinatorQueue.poll()) != null) {
            if (obj instanceof BatchContext batchContext) {
                if (batchContext.lastTid > lastTid)
                    lastTid = batchContext.lastTid;
            }
        }

        System.out.println("Experiment finished: \n"+
                " Throughput (tx/s) "+ (lastTid/(max_time/1000)));

    }

    private static List<TransactionWorker> setupTransactionWorkers(int numWorkers, int maxNumTidBatch,
                                                                   Queue<Object> coordinatorQueue,
                                                                   List<ConcurrentLinkedDeque<TransactionInput>> txInputQueues){
        List<TransactionWorker> txWorkers = new ArrayList<>();
        var vmsMetadataMap = new HashMap<String, VmsNode>();
        vmsMetadataMap.put( "product", new VmsNode("localhost", 8080, "product", 0, 0, 0, null, null, null));

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG updateProductDag = TransactionBootstrap.name("update_product")
                .input("a", "product", "update_product")
                .terminal("b", "product", "a")
                .build();
        transactionMap.put(updateProductDag.name, updateProductDag);

        Map<String, VmsNode[]> vmsNodePerDAG = new HashMap<>();
        for(var dag : transactionMap.entrySet()) {
            vmsNodePerDAG.put(dag.getKey(), BatchAlgo.buildTransactionDagVmsList(dag.getValue(), vmsMetadataMap));
        }

        Map<String,IVmsWorker> workers = new HashMap<>();
        workers.put("product", new NoOpVmsWorker());

        // generic algorithm to handle N number of transaction workers
        int idx = 1;
        long initTid = 1;

        var firstPrecedenceInputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>>();
        var precedenceMapInputQueue = firstPrecedenceInputQueue;
        ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>> precedenceMapOutputQueue;

        Map<String, TransactionWorker.PrecendenceInfo> precedenceMap = new HashMap<>();
        precedenceMap.put("product", new TransactionWorker.PrecendenceInfo(0, 0, 0));
        precedenceMapInputQueue.add(precedenceMap);

        var serdesProxy = VmsSerdesProxyBuilder.build();
        do {
            if(idx < numWorkers){
                precedenceMapOutputQueue = new ConcurrentLinkedDeque<>();
            } else {
                precedenceMapOutputQueue = firstPrecedenceInputQueue;
            }
            var txWorker = TransactionWorker.build(idx, txInputQueues.get(idx-1), initTid, maxNumTidBatch, 1000,
                    numWorkers, precedenceMapInputQueue, precedenceMapOutputQueue, transactionMap,
                    vmsNodePerDAG, workers, coordinatorQueue, serdesProxy);
            initTid = initTid + maxNumTidBatch;
            precedenceMapInputQueue = precedenceMapOutputQueue;
            idx++;
            txWorkers.add(txWorker);
        } while (idx <= numWorkers);
        return txWorkers;
    }

    private static class NoOpVmsWorker implements IVmsWorker {
        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) { }
        @Override
        public void queueMessage(Object message) { }
    }

}
