package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoader;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcHttpHandler;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;

import java.util.*;

public final class Main {
    public static void main(String[] ignoredArgs) throws NoSuchFieldException, IllegalAccessException {
        Properties properties = ConfigUtils.loadProperties();
        loadCoordinator(properties);
        // main menu
        menu();
    }

    private static void menu() throws NoSuchFieldException, IllegalAccessException {
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
                    System.out.println("Option 1: Create tables selected.");
                    tables = StorageUtils.createTables(metadata);
                    break;
                case "2":
                    System.out.println("Option 2: Ingest data selected.");
                    if(tables == null) {
                        System.out.println("Loading tables from disk...");
                        tables = StorageUtils.loadTables(metadata);
                    }
                    DataLoader.load(tables, metadata.entityHandlerMap());
                    break;
                case "3":
                    System.out.println("Option 3: Create workload selected.");
                    input = WorkloadUtils.createWorkload();
                    break;
                case "4":
                    System.out.println("Option 4: Submit workload selected.");
                    if(input == null){
                        System.out.println("Loading workload from disk...");
                        input = WorkloadUtils.loadWorkloadData();
                    }
                    WorkloadUtils.submitWorkload(input);
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

    private static void printMenu() {
        System.out.println("\n=== Main Menu ===");
        System.out.println("1. Create tables");
        System.out.println("2. Ingest data");
        System.out.println("3. Create workload");
        System.out.println("4. Submit workload");
        System.out.println("0. Exit");
    }

    private static void loadCoordinator(Properties properties) {
        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG newOrderDag = TransactionBootstrap.name("new_order")
                .input("a", "warehouse", "new-order-ware-in")
                .internal("b", "inventory", "new-order-ware-out", "a")
                .terminal("c", "order", "b")
                .build();
        transactionMap.put(newOrderDag.name, newOrderDag);
        Map<String, IdentifiableNode> starterVMSs = getVmsMap(properties);
        Coordinator coordinator = Coordinator.build(properties, starterVMSs, transactionMap, TPCcHttpHandler::new);
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();
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
