package dk.ku.di.dms.vms.tpcc.proxy.workload;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.nuRand;
import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.randomNumber;
import static dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants.*;
import static java.lang.System.Logger.Level.*;

public final class WorkloadUtils {

    private static final System.Logger LOGGER = System.getLogger(WorkloadUtils.class.getName());

    private static final Schema SCHEMA = new Schema(
            new String[]{ "w_id", "d_id", "c_id", "itemIds", "supWares", "qty", "allLocal" },
            new DataType[]{
                    DataType.INT, DataType.INT, DataType.INT, DataType.INT_ARRAY,
                    DataType.INT_ARRAY, DataType.INT_ARRAY, DataType.BOOL
            },
            new int[]{},
            new ConstraintReference[]{},
            false
    );

    private static void write(long pos, Object[] record) {
        long currAddress = pos;
        for (int index = 0; index < SCHEMA.columnOffset().length; index++) {
            DataType dt = SCHEMA.columnDataType(index);
            DataTypeUtils.callWriteFunction(currAddress, dt, record[index]);
            currAddress += dt.value;
        }
    }

    private static Object[] read(long address){
        Object[] record = new Object[SCHEMA.columnOffset().length];
        long currAddress = address;
        for(int i = 0; i < SCHEMA.columnOffset().length; i++) {
            DataType dt = SCHEMA.columnDataType(i);
            record[i] = DataTypeUtils.getValue(dt, currAddress);
            currAddress += dt.value;
        }
        return record;
    }

    public static void runExperiment(Coordinator coordinator, List<NewOrderWareIn> input, int numWorkers, int runTime) {

        // provide a consumer to avoid depending on the coordinator
        Function<NewOrderWareIn, Long> func = newOrderInputBuilder(coordinator);

        coordinator.registerBatchCommitConsumer((tid)-> BATCH_TO_FINISHED_TS_MAP.put(
                (long) BATCH_TO_FINISHED_TS_MAP.size()+1,
                new BatchStats( BATCH_TO_FINISHED_TS_MAP.size()+1, tid, System.currentTimeMillis())));

        Map<Long,List<Long>>[] submitted = WorkloadUtils.submitWorkload(input, numWorkers, runTime, func);

        List<Long> allLatencies = new ArrayList<>();

        // calculate latency based on the batch
        for(var workerEntry : submitted){
            for(var batchEntry : BATCH_TO_FINISHED_TS_MAP.entrySet()) {
                if(!workerEntry.containsKey(batchEntry.getKey())) continue;
                var initTsEntries = workerEntry.get(batchEntry.getKey());
                for (var initTsEntry : initTsEntries){
                    allLatencies.add(batchEntry.getValue().endTs - initTsEntry);
                }
            }
        }

        allLatencies.sort(null);
        double percentile = PercentileCalculator.calculatePercentile(allLatencies, 0.75);

        // var result = new Result(percentile, Map.copyOf(BATCH_TO_FINISHED_TS_MAP));

        resetBatchToFinishedTsMap();

        // return result;
    }

    public record Result(double percentile, Map<Long, Long> batchTs){}

    private static void resetBatchToFinishedTsMap(){
        BATCH_TO_FINISHED_TS_MAP.clear();
        //BATCH_TO_FINISHED_TS_MAP.put(0L, 0L);
    }

    private static final ConcurrentHashMap<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();

    private record BatchStats(long batchId, long lastTid, long endTs){}

    private static Function<NewOrderWareIn, Long> newOrderInputBuilder(final Coordinator coordinator) {
        return newOrderWareIn -> {
            TransactionInput.Event eventPayload = new TransactionInput.Event("new-order-ware-in", newOrderWareIn.toString());
            TransactionInput txInput = new TransactionInput("new_order", eventPayload);
            coordinator.queueTransactionInput(txInput);
            return (long) BATCH_TO_FINISHED_TS_MAP.size() + 1;
        };
    }

    @SuppressWarnings("unchecked")
    public static Map<Long,List<Long>>[] submitWorkload(List<NewOrderWareIn> input, int numWorkers, int runTime, Function<NewOrderWareIn, Long> func) {
        List<List<NewOrderWareIn>> inputLists;
        if(numWorkers > 1) {
            inputLists = partition(input, numWorkers);
        } else {
            inputLists = List.of(input);
        }

        LOGGER.log(INFO, "Submitting "+input.size()+" transactions through "+numWorkers+" worker(s)");

        CountDownLatch allThreadsStart = new CountDownLatch(numWorkers);
        CountDownLatch allThreadsAreDone = new CountDownLatch(numWorkers);

        Map<Long,List<Long>>[] submittedArray = new Map[numWorkers];

        for(int i = 0; i < numWorkers; i++) {
            final List<NewOrderWareIn> workerInput = inputLists.get(i);
            int finalI = i;
            Thread thread = new Thread(()->
                    submittedArray[finalI] = Worker.run(allThreadsStart, allThreadsAreDone, workerInput, runTime, func));
            thread.start();
        }

        LOGGER.log(INFO,"Experiment main going to wait for the workers to finish.");
        try {
            if (!allThreadsAreDone.await(runTime * 2L, TimeUnit.MILLISECONDS)) {
                LOGGER.log(ERROR,"Latch has not reached zero. Something wrong with the worker(s)");
            } else{
                LOGGER.log(INFO,"Experiment main woke up!");
            }
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }

        return submittedArray;
    }

    private static final class Worker {

        public static Map<Long,List<Long>> run(CountDownLatch allThreadsStart,
                                         CountDownLatch allThreadsAreDone,
                                         List<NewOrderWareIn> input, int runTime,
                                         Function<NewOrderWareIn, Long> func) {
            Map<Long,List<Long>> startTsMap = new HashMap<>();
            long threadId = Thread.currentThread().threadId();

            allThreadsStart.countDown();
            try {
                allThreadsStart.await();
            } catch (InterruptedException e) {
                LOGGER.log(ERROR, "Thread ID "+threadId+" failed to await");
                throw new RuntimeException(e);
            }

            LOGGER.log(INFO,"Thread ID " + threadId + " started");
            int idx = 0;

            long currentTs = System.currentTimeMillis();
            long endTs = System.currentTimeMillis() + runTime;
            do {
                try {
                    long batchId = func.apply(input.get(idx));
                    if(!startTsMap.containsKey(batchId)){
                        startTsMap.put(batchId, new ArrayList<>());
                    }
                    startTsMap.get(batchId).add(currentTs);
                    idx++;
                } catch (Exception e) {
                    idx = 0;
                    /*
                    LOGGER.log(ERROR,"Exception in Thread ID: " + (e.getMessage() == null ? "No message" : e.getMessage()));
                    if(idx >= input.size()){
                        allThreadsAreDone.countDown();
                        LOGGER.log(WARNING,"Number of input events "+input.size()+" are not enough for runtime "+runTime+" ms");
                        break;
                    }
                    */
                }
                currentTs = System.currentTimeMillis();
            } while (currentTs < endTs);

            allThreadsAreDone.countDown();
            return startTsMap;
        }
    }

    private static List<List<NewOrderWareIn>> partition(List<NewOrderWareIn> input, int numWorkers){
        List<List<NewOrderWareIn>> partitions = new ArrayList<>();
        int totalSize = input.size();
        int basePartitionSize = totalSize / numWorkers;
        int remainder = totalSize % numWorkers;
        int startIndex = 0;
        for (int i = 0; i < numWorkers; i++) {
            int endIndex = startIndex + basePartitionSize + (remainder-- > 0 ? 1 : 0);
            partitions.add(new ArrayList<>(input.subList(startIndex, Math.min(endIndex, totalSize))));
            startIndex = endIndex;
        }
        return partitions;
    }

    public static List<NewOrderWareIn> loadWorkloadData(){
        AppendOnlyBuffer buffer = EmbedMetadataLoader.loadAppendOnlyBufferUnknownSize("new_order_input");
        // calculate number of entries (i.e., transactions)
        int numTransactions = (int) buffer.size() / SCHEMA.getRecordSize();
        LOGGER.log(INFO, "Starting loading "+numTransactions+" from disk...");
        long initTs = System.currentTimeMillis();
        List<NewOrderWareIn> input = new ArrayList<>(numTransactions);
        for(int txIdx = 1; txIdx <= numTransactions; txIdx++) {
            Object[] newOrderInput = read(buffer.nextOffset());
            input.add(parseRecordIntoEntity(newOrderInput));
            buffer.forwardOffset(SCHEMA.getRecordSize());
        }
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished loading "+numTransactions+" from disk in "+(endTs-initTs)+" ms");
        return input;
    }

    public static List<NewOrderWareIn> createWorkload(int numWare, int numTransactions){
        LOGGER.log(INFO, "Starting the generation of "+numTransactions+"...");
        long initTs = System.currentTimeMillis();
        List<NewOrderWareIn> input = new ArrayList<>(numTransactions);
        AppendOnlyBuffer buffer = EmbedMetadataLoader.loadAppendOnlyBuffer(numTransactions, SCHEMA.getRecordSize(),"new_order_input", true);
        for(int txIdx = 1; txIdx <= numTransactions; txIdx++) {
            int w_id = randomNumber(1, numWare);
            Object[] newOrderInput = generateNewOrder(w_id, numWare);
            input.add(parseRecordIntoEntity(newOrderInput));
            write(buffer.nextOffset(), newOrderInput);
            buffer.forwardOffset(SCHEMA.getRecordSize());
        }
        buffer.force();
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished generating "+numTransactions+" in "+(endTs-initTs)+" ms");
        return input;
    }

    public static Coordinator loadCoordinator(Properties properties) {
        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG newOrderDag = TransactionBootstrap.name("new_order")
                .input("a", "warehouse", "new-order-ware-in")
                .internal("b", "inventory", "new-order-ware-out", "a")
                .terminal("c", "order", "b")
                .build();
        transactionMap.put(newOrderDag.name, newOrderDag);
        Map<String, IdentifiableNode> starterVMSs = getVmsMap(properties);
        Coordinator coordinator = Coordinator.build(properties, starterVMSs, transactionMap, (ignored1) -> IHttpHandler.DEFAULT);
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();
        return coordinator;
    }

    private static Map<String, IdentifiableNode> getVmsMap(Properties properties) {
        String warehouseHost = properties.getProperty("warehouse_host");
        String inventoryHost = properties.getProperty("inventory_host");
        String orderHost = properties.getProperty("order_host");
        if(warehouseHost == null) throw new RuntimeException("Warehouse host is null");
        if(inventoryHost == null) throw new RuntimeException("Inventory host is null");
        if(orderHost == null) throw new RuntimeException("Order host is null");
        IdentifiableNode warehouseAddress = new IdentifiableNode("warehouse", warehouseHost, 8001);
        IdentifiableNode inventoryAddress = new IdentifiableNode("inventory", inventoryHost, 8002);
        IdentifiableNode orderAddress = new IdentifiableNode("order", orderHost, 8003);
        Map<String, IdentifiableNode> starterVMSs = new HashMap<>();
        starterVMSs.putIfAbsent(warehouseAddress.identifier, warehouseAddress);
        starterVMSs.putIfAbsent(inventoryAddress.identifier, inventoryAddress);
        starterVMSs.putIfAbsent(orderAddress.identifier, orderAddress);
        return starterVMSs;
    }

    private static NewOrderWareIn parseRecordIntoEntity(Object[] newOrderInput) {
        return new NewOrderWareIn(
                (int) newOrderInput[0],
                (int) newOrderInput[1],
                (int) newOrderInput[2],
                (int[]) newOrderInput[3],
                (int[]) newOrderInput[4],
                (int[]) newOrderInput[5],
                (boolean) newOrderInput[6]
        );
    }

    private static Object[] generateNewOrder(int w_id, int num_ware){
        int d_id;
        int c_id;
        int ol_cnt;
        int all_local = 1;
        int not_found = NUM_ITEMS + 1;
        int rbk;

        int max_num_items_per_order_ = Math.min(MAX_NUM_ITEMS_PER_ORDER, NUM_ITEMS);
        int min_num_items_per_order_ = Math.min(5, max_num_items_per_order_);

        d_id = randomNumber(1, NUM_DIST_PER_WARE);
        c_id = nuRand(1023, 1, NUM_CUST_PER_DIST);

        ol_cnt = randomNumber(min_num_items_per_order_, max_num_items_per_order_);
        rbk = randomNumber(1, 100);

        int[] itemIds = new int[ol_cnt];
        int[] supWares = new int[ol_cnt];
        int[] qty = new int[ol_cnt];

        for (int i = 0; i < ol_cnt; i++) {
            int item_ = nuRand(8191, 1, NUM_ITEMS);

            // avoid duplicate items
            while(foundItem(itemIds, i, item_)){
                item_ = nuRand(8191, 1, NUM_ITEMS);
            }
            itemIds[i] = item_;

            if(FORCE_ABORTS) {
                if ((i == ol_cnt - 1) && (rbk == 1)) {
                    // this can lead to exception and then abort in app code
                    itemIds[i] = not_found;
                }
            }

            if (ALLOW_MULTI_WAREHOUSE_TX) {
                if (randomNumber(1, 100) != 1) {
                    supWares[i] = w_id;
                } else {
                    supWares[i] = otherWare(num_ware, w_id);
                    all_local = 0;
                }
            } else {
                supWares[i] = w_id;
            }
            qty[i] = randomNumber(1, 10);
        }

        return new Object[]{ w_id, d_id, c_id, itemIds, supWares, qty, all_local == 1 };
    }

    private static boolean foundItem(int[] itemIds, int length, int value){
        if(length == 0) return false;
        for(int i = 0; i < length; i++){
            if(itemIds[i] == value) return true;
        }
        return false;
    }

    /**
     * Based on <a href="https://github.com/AgilData/tpcc/blob/master/src/main/java/com/codefutures/tpcc/Driver.java#L310">AgilData</a>
     */
    private static int otherWare(int num_ware, int home_ware) {
        int tmp;
        if (num_ware == 1) return home_ware;
        do {
            tmp = randomNumber(1, num_ware);
        } while (tmp == home_ware);
        return tmp;
    }

}
