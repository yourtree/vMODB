package dk.ku.di.dms.vms.tpcc.proxy.experiment;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

public final class ExperimentUtils {

    public static void runExperiment(Coordinator coordinator, List<NewOrderWareIn> input, int numWorkers, int runTime, int warmUp) {

        // provide a consumer to avoid depending on the coordinator
        Function<NewOrderWareIn, Long> func = newOrderInputBuilder(coordinator);

        coordinator.registerBatchCommitConsumer((tid)-> BATCH_TO_FINISHED_TS_MAP.put(
                (long) BATCH_TO_FINISHED_TS_MAP.size()+1,
                new BatchStats( BATCH_TO_FINISHED_TS_MAP.size()+1, tid, System.currentTimeMillis())));

        int newRuntime = runTime + warmUp;

        WorkloadUtils.WorkloadStats workloadStats = WorkloadUtils.submitWorkload(input, numWorkers, newRuntime, func);

        coordinator.stop();

        long endTs = workloadStats.initTs() + newRuntime;
        long initTs = workloadStats.initTs() + warmUp;
        int numCompleted = 0;
        List<Long> allLatencies = new ArrayList<>();
        // calculate latency based on the batch
        for(var workerEntry : workloadStats.submitted()){
            for(var batchEntry : BATCH_TO_FINISHED_TS_MAP.entrySet()) {
                if(batchEntry.getValue().endTs > endTs) continue;
                if(!workerEntry.containsKey(batchEntry.getKey())) continue;
                var initTsEntries = workerEntry.get(batchEntry.getKey());
                for (var initTsEntry : initTsEntries){
                    if(initTsEntry >= initTs) {
                        numCompleted += 1;
                        allLatencies.add(batchEntry.getValue().endTs - initTsEntry);
                    }
                }
            }
        }

        double average = allLatencies.stream()
                .mapToLong(Long::longValue)
                .average().orElse(0.0);

        allLatencies.sort(null);
        double percentile_50 = PercentileCalculator.calculatePercentile(allLatencies, 0.50);
        double percentile_75 = PercentileCalculator.calculatePercentile(allLatencies, 0.75);
        double percentile_90 = PercentileCalculator.calculatePercentile(allLatencies, 0.90);
        long txPerSec = numCompleted / (runTime / 1000);

        System.out.println("Average latency: "+ average);
        System.out.println("Latency at 50th percentile: "+ percentile_50);
        System.out.println("Latency at 75th percentile: "+ percentile_75);
        System.out.println("Latency at 90th percentile: "+ percentile_90);
        System.out.println("Number of completed transactions: "+ numCompleted);
        System.out.println("Transactions per second: "+txPerSec);
        // var result = new Result(percentile, Map.copyOf(BATCH_TO_FINISHED_TS_MAP));

        resetBatchToFinishedTsMap();
        // return result;
    }

    private static void resetBatchToFinishedTsMap(){
        BATCH_TO_FINISHED_TS_MAP.clear();
        //BATCH_TO_FINISHED_TS_MAP.put(0L, 0L);
    }

    private static final ConcurrentSkipListMap<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentSkipListMap<>();

    private record BatchStats(long batchId, long lastTid, long endTs){}

    private static Function<NewOrderWareIn, Long> newOrderInputBuilder(final Coordinator coordinator) {
        return newOrderWareIn -> {
            TransactionInput.Event eventPayload = new TransactionInput.Event("new-order-ware-in", newOrderWareIn.toString());
            TransactionInput txInput = new TransactionInput("new_order", eventPayload);
            coordinator.queueTransactionInput(txInput);
            return (long) BATCH_TO_FINISHED_TS_MAP.size() + 1;
        };
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

}
