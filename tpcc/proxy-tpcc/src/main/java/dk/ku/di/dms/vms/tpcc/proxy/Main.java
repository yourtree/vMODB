package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoader;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

public final class Main {

    private static final Properties PROPERTIES = ConfigUtils.loadProperties();

    public static void main(String[] ignoredArgs) throws NoSuchFieldException, IllegalAccessException {
        menu();
    }

    private static void menu() throws NoSuchFieldException, IllegalAccessException {
        Coordinator coordinator = null;
        int numWare;
        int numWorkers;
        Map<String, UniqueHashBufferIndex> tables = null;
        List<NewOrderWareIn> input = null;
        StorageUtils.EntityMetadata metadata = StorageUtils.loadEntityMetadata();
        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        while (running) {
            printMenu();
            System.out.print("Enter your choice: ");
            String choice = scanner.nextLine();
            System.out.println("You chose option: " + choice);
            switch (choice) {
                case "1":
                    System.out.println("Option 1: \"Create tables in disk\" selected.");
                    System.out.print("Enter number of warehouses: ");
                    numWare = Integer.parseInt(scanner.nextLine());
                    System.out.println("Creating tables with "+numWare+" warehouses...");
                    tables = StorageUtils.createTables(metadata, numWare);
                    System.out.println("Tables created!");
                    break;
                case "2":
                    System.out.println("Option 2: \"Load services with data from tables in disk\" selected.");
                    // number of worker threads
                    System.out.print("Enter number of workers: ");
                    numWorkers = Integer.parseInt(scanner.nextLine());
                    if(tables == null) {
                        System.out.println("Loading tables from disk...");
                        // the number of warehouses must be exactly the same otherwise lead to errors in reading from files
                        numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");
                        tables = StorageUtils.loadTables(metadata, numWare);
                    }
                    DataLoader.load(tables, metadata.entityHandlerMap(), numWorkers);
                    break;
                case "3":
                    System.out.println("Option 3: \"Create workload\" selected.");
                    System.out.print("Enter number of warehouses: ");
                    numWare = Integer.parseInt(scanner.nextLine());
                    System.out.print("Enter number of transactions: ");
                    String numTxnStr = scanner.nextLine();
                    input = WorkloadUtils.createWorkload(numWare, Integer.parseInt(numTxnStr));
                    break;
                case "4":
                    System.out.println("Option 4: \"Submit workload\" selected.");
                    // number of worker threads
                    System.out.print("Enter number of workers: ");
                    numWorkers = Integer.parseInt(scanner.nextLine());
                    System.out.print("Enter duration (ms): [0 for default to 10s]");
                    int runTime = Integer.parseInt(scanner.nextLine());
                    if(runTime == 0) runTime = 10000;
                    if(input == null){
                        System.out.println("Loading workload from disk...");
                        input = WorkloadUtils.loadWorkloadData();
                    }
                    // load coordinator
                    if(coordinator == null){
                        coordinator = loadCoordinator();
                        // wait for all starter VMSes to connect
                        int numConnected;
                        do {
                            numConnected = coordinator.getConnectedVMSs().size();
                        } while (numConnected < 3);
                    }
                    // provide a consumer to avoid depending on the coordinator
                    Function<NewOrderWareIn, Long> func = newOrderInputBuilder(coordinator);

                    coordinator.registerBatchCommitConsumer((tid)-> BATCH_TO_FINISHED_TS_MAP.put(tid, System.currentTimeMillis()));

                    var submitted = WorkloadUtils.submitWorkload(input, numWorkers, runTime, func);

                    // TODO calculate latency based on the batch... 1-2, 2-3, ... the first is naturally filtered out

                    break;
                case "0":
                    System.out.println("Exiting the application. Goodbye!");
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
        scanner.close();
    }

    private static final ConcurrentSkipListMap<Long, Long> BATCH_TO_FINISHED_TS_MAP = new ConcurrentSkipListMap<>();

    private static Function<NewOrderWareIn, Long> newOrderInputBuilder(Coordinator coordinator) {
        return newOrderWareIn -> {
            TransactionInput.Event eventPayload = new TransactionInput.Event("new_order", newOrderWareIn.toString());
            TransactionInput txInput = new TransactionInput("new_order", eventPayload);
            coordinator.queueTransactionInput(txInput);
            return BATCH_TO_FINISHED_TS_MAP.lastKey();
        };
    }

    private static void printMenu() {
        System.out.println("\n=== Main Menu ===");
        System.out.println("1. Create tables in disk");
        System.out.println("2. Load services with data from tables in disk");
        System.out.println("3. Create workload");
        System.out.println("4. Submit workload");
        System.out.println("0. Exit");
    }

    private static Coordinator loadCoordinator() {
        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG newOrderDag = TransactionBootstrap.name("new_order")
                .input("a", "warehouse", "new-order-ware-in")
                .internal("b", "inventory", "new-order-ware-out", "a")
                .terminal("c", "order", "b")
                .build();
        transactionMap.put(newOrderDag.name, newOrderDag);
        Map<String, IdentifiableNode> starterVMSs = getVmsMap();
        Coordinator coordinator = Coordinator.build(PROPERTIES, starterVMSs, transactionMap, (ignored1) -> IHttpHandler.DEFAULT);
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();
        return coordinator;
    }

    private static Map<String, IdentifiableNode> getVmsMap() {
        String warehouseHost = PROPERTIES.getProperty("warehouse_host");
        String inventoryHost = PROPERTIES.getProperty("inventory_host");
        String orderHost = PROPERTIES.getProperty("order_host");
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
