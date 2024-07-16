package dk.ku.di.dms.vms.marketplace.proxy;

import dk.ku.di.dms.vms.coordinator.batch.BatchAlgo;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionWorker;
import dk.ku.di.dms.vms.coordinator.vms.IVmsWorker;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;

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
        // must ideally scale together
        int num_vms_workers = Integer.parseInt( properties.getProperty("num_vms_workers") );
        int num_transaction_workers = Integer.parseInt( properties.getProperty("num_transaction_workers") );
        // can potentially hide the wait time introduced by the "ring"
        int num_max_transactions_batch = Integer.parseInt( properties.getProperty("num_max_transactions_batch") );
        int max_time = Integer.parseInt( properties.getProperty("max_time") );

        System.out.println("Experiment config: \n"+
                " Num vms workers "+ (num_transaction_workers)+"\n"+
                " Num transaction workers "+ (num_transaction_workers)+"\n"+
                " Num max transaction batch "+ (num_max_transactions_batch)+"\n"+
                " Max time "+ (max_time)+"\n"
        );

        int NUM_QUEUED_TRANSACTIONS = 5000000;

        List<ConcurrentLinkedDeque<TransactionInput>> txInputQueues = new ArrayList<>(num_transaction_workers);

        // fill each queue with proper number of transaction inputs
        for (int i = 1; i <= num_transaction_workers; i++) {
           var inputQueue = new ConcurrentLinkedDeque<TransactionInput>();
           txInputQueues.add(inputQueue);
           System.out.println("Generating "+NUM_QUEUED_TRANSACTIONS+" transactions for worker # "+i);
           for(int j = 1; j <= NUM_QUEUED_TRANSACTIONS; j++) {
               inputQueue.add(new TransactionInput("update_product",
                       new TransactionInput.Event("update_product", String.valueOf(i))));
           }
        }

        Deque<Object> coordinatorQueue = new ConcurrentLinkedDeque<>();

        BaseMicroBenchVmsWorker vmsWorker;
        if(num_vms_workers > 1) {
            vmsWorker = new ComplexMicroBenchVmsWorker(num_vms_workers);
        } else {
            vmsWorker = new SimpleMicroBenchVmsWorker();
        }

        System.out.println("Setting up "+num_transaction_workers+" workers");
        List<TransactionWorker> workers = setupTransactionWorkers(num_transaction_workers, num_max_transactions_batch,
                vmsWorker, coordinatorQueue, txInputQueues);

        System.out.println("Initializing "+num_transaction_workers+" worker threads");
        var threadFactory = Thread.ofPlatform().factory();
        for (TransactionWorker worker : workers) {
            threadFactory.newThread(worker).start();
        }

        // 10 seconds - 1 second
        sleep(max_time-1000);

        for (TransactionWorker worker : workers) {
            worker.stop();
        }

        long numPayloads = vmsWorker.getNumPayloads();
        long numBatches = coordinatorQueue.size();

        //  add batches per second. count num of batches in the queue
        System.out.println("Experiment finished: \n"+
                " Payloads per second: "+ (numPayloads/(max_time/1000))+"\n"+
                " Batches per second: "+ (numBatches/(max_time/1000))
        );
    }

    private static abstract class BaseMicroBenchVmsWorker implements IVmsWorker {
        abstract int getNumPayloads();
    }

    private static class ComplexMicroBenchVmsWorker extends BaseMicroBenchVmsWorker {

        private final List<Queue<TransactionEvent.PayloadRaw>> payloadQueues;
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private final Queue<Object> messageQueue = new ConcurrentLinkedDeque<>();
        private final int numVmsWorkers;

        public ComplexMicroBenchVmsWorker(int numVmsWorkers){
            this.numVmsWorkers = numVmsWorkers;
            this.payloadQueues = new ArrayList<>(numVmsWorkers);
            for (int i = 0; i < numVmsWorkers; i++) {
                payloadQueues.add(new ConcurrentLinkedDeque<>());
            }
        }

        @Override
        int getNumPayloads() {
            int sum = 0;
            for (Queue<TransactionEvent.PayloadRaw> queue: payloadQueues) {
                sum += queue.size();
            }
            return sum;
        }

        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
            int idx = ThreadLocalRandom.current().nextInt(0, this.numVmsWorkers);
            this.payloadQueues.get(idx).add(payloadRaw);
        }

        @Override
        public void queueMessage(Object message) {
            this.messageQueue.add(message);
        }
    }

    /**
     * Queuing payloads can lead to contention for multiple transaction workers
     */
    private static class SimpleMicroBenchVmsWorker extends BaseMicroBenchVmsWorker {

        private final Queue<TransactionEvent.PayloadRaw> payloadQueue = new ConcurrentLinkedDeque<>();
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private final Queue<Object> messageQueue = new ConcurrentLinkedDeque<>();

        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
            // must add the overhead of adding to vms worker queue at least
            this.payloadQueue.add(payloadRaw);
        }
        @Override
        public void queueMessage(Object message) {
            this.messageQueue.add(message);
        }

        public int getNumPayloads(){
            return this.payloadQueue.size();
        }

    }

    private static List<TransactionWorker> setupTransactionWorkers(int numWorkers, int maxNumTidBatch,
                                                                   IVmsWorker vmsWorker,
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
        workers.put("product", vmsWorker);

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

}
