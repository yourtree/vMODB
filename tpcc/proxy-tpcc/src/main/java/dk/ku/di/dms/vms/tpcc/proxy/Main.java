package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;

import java.util.*;

public final class Main {

    private static final Properties PROPERTIES = ConfigUtils.loadProperties();

    public static void main(String[] ignoredArgs) throws NoSuchFieldException, IllegalAccessException {
        menu();
    }

    private static void menu() throws NoSuchFieldException, IllegalAccessException {
        Coordinator coordinator = null;
        int numWare = 0;
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
                    if(tables == null) {
                        System.out.println("Loading tables from disk...");
                        // the number of warehouses must be exactly the same otherwise lead to errors in reading from files
                        numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");
                        tables = StorageUtils.mapTablesInDisk(metadata, numWare);
                    }
                    Map<String, Queue<String>> tablesInMem = DataLoadUtils.loadTablesInMemory(tables, metadata.entityHandlerMap());
                    DataLoadUtils.ingestData(tablesInMem);
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

                    System.out.print("Enter warm up period (ms):");
                    int warmUp = Integer.parseInt(scanner.nextLine());

                    if(input == null){
                        System.out.println("Loading workload from disk...");
                        input = WorkloadUtils.loadWorkloadData();
                    }

                    // load coordinator
                    if(coordinator == null){
                        coordinator = ExperimentUtils.loadCoordinator(PROPERTIES);
                        // wait for all starter VMSes to connect
                        int numConnected;
                        do {
                            numConnected = coordinator.getConnectedVMSs().size();
                        } while (numConnected < 3);
                    }

                    var expStats = ExperimentUtils.runExperiment(coordinator, input, numWorkers, runTime, warmUp);

                    ExperimentUtils.writeResultsToFile(numWare, expStats, numWorkers, runTime, warmUp);

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
        System.out.println("1. Create tables in disk");
        System.out.println("2. Load services with data from tables in disk");
        System.out.println("3. Create workload");
        System.out.println("4. Submit workload");
        System.out.println("0. Exit");
    }

}
