package dk.ku.di.dms.vms.tpcc.proxy.experiment;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.lang.System.Logger.Level.INFO;

public final class ExperimentUtils {

    private static final System.Logger LOGGER = System.getLogger(ExperimentUtils.class.getName());

    private static boolean CONSUMER_REGISTERED = false;

    public static ExperimentStats runExperiment(Coordinator coordinator, List<Iterator<NewOrderWareIn>> input, int runTime, int warmUp) {

        // provide a consumer to avoid depending on the coordinator
        Function<NewOrderWareIn, Long> func = newOrderInputBuilder(coordinator);

        if(!CONSUMER_REGISTERED) {
            coordinator.registerBatchCommitConsumer((tid) -> BATCH_TO_FINISHED_TS_MAP.put(
                    (long) BATCH_TO_FINISHED_TS_MAP.size() + 1,
                    new BatchStats(BATCH_TO_FINISHED_TS_MAP.size() + 1, tid, System.currentTimeMillis())));
            CONSUMER_REGISTERED = true;
        }

        int newRuntime = runTime + warmUp;

        WorkloadUtils.WorkloadStats workloadStats = WorkloadUtils.submitWorkload(input, newRuntime, func);

        // avoid submitting after experiment termination
        coordinator.clearTransactionInputs();
        LOGGER.log(INFO,"Transaction input queue(s) cleared.");

        long endTs = workloadStats.initTs() + newRuntime;
        long initTs = workloadStats.initTs() + warmUp;
        int numCompletedWithWarmUp = 0;
        int numCompleted = 0;
        List<Long> allLatencies = new ArrayList<>();
        // calculate latency based on the batch
        for(var workerEntry : workloadStats.submitted()){
            for(var batchEntry : BATCH_TO_FINISHED_TS_MAP.entrySet()) {
                if(batchEntry.getValue().endTs > endTs) continue;
                if(!workerEntry.containsKey(batchEntry.getKey())) continue;
                var initTsEntries = workerEntry.get(batchEntry.getKey());
                numCompletedWithWarmUp += initTsEntries.size();
                for (var initTsEntry : initTsEntries){
                    if(initTsEntry >= initTs) {
                        numCompleted += 1;
                        allLatencies.add(batchEntry.getValue().endTs - initTsEntry);
                    }
                }
            }
        }

        double average = allLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        allLatencies.sort(null);
        double percentile_50 = PercentileCalculator.calculatePercentile(allLatencies, 0.50);
        double percentile_75 = PercentileCalculator.calculatePercentile(allLatencies, 0.75);
        double percentile_90 = PercentileCalculator.calculatePercentile(allLatencies, 0.90);
        long txPerSec = numCompleted / (runTime / 1000);

        System.out.println("Average latency: "+ average);
        System.out.println("Latency at 50th percentile: "+ percentile_50);
        System.out.println("Latency at 75th percentile: "+ percentile_75);
        System.out.println("Latency at 90th percentile: "+ percentile_90);
        System.out.println("Number of completed transactions (with warm up): "+ numCompletedWithWarmUp);
        System.out.println("Number of completed transactions: "+ numCompleted);
        System.out.println("Transactions per second: "+txPerSec);
        System.out.println();

        resetBatchToFinishedTsMap();

        return new ExperimentStats(workloadStats.initTs(), numCompletedWithWarmUp, numCompleted, txPerSec, average, percentile_50, percentile_75, percentile_90);
    }

    public record ExperimentStats(long initTs, int numCompletedWithWarmUp, int numCompleted, long txPerSec, double average,
                                   double percentile_50, double percentile_75, double percentile_90){}

    public static void writeResultsToFile(int numWare, ExperimentStats expStats, int runTime, int warmUp){
        LocalDateTime time = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(expStats.initTs),
                ZoneId.systemDefault()
        );
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd_MM_yy_HH_mm_ss");
        String formattedDate = time.format(formatter);
        String fileName = "tpcc_" + formattedDate + ".txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write("======= TPC-C in vMODB =======");
            writer.newLine();
            writer.write("Experiment start: " + formattedDate);
            writer.newLine();
            writer.write("Experiment duration (ms): " + runTime);
            writer.newLine();
            writer.write("Experiment warm up (ms): " + warmUp);
            writer.newLine();
            writer.write("Number of warehouses: " + numWare);
            writer.newLine();
            writer.newLine();
            writer.write("Average latency: "+ expStats.average);
            writer.newLine();
            writer.write("Latency at 50th percentile: "+ expStats.percentile_50);
            writer.newLine();
            writer.write("Latency at 75th percentile: "+ expStats.percentile_75);
            writer.newLine();
            writer.write("Latency at 90th percentile: "+ expStats.percentile_90);
            writer.newLine();
            writer.write("Number of completed transactions (with warm up): "+ expStats.numCompletedWithWarmUp);
            writer.newLine();
            writer.write("Number of completed transactions: "+ expStats.numCompleted);
            writer.newLine();
            writer.write("Transactions per second: "+expStats.txPerSec);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void resetBatchToFinishedTsMap(){
        BATCH_TO_FINISHED_TS_MAP.clear();
    }

    private static final Map<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();

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
