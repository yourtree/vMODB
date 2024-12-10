package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.QueueTableIterator;
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
        Map<String, UniqueHashBufferIndex> tables = null;
        List<Iterator<NewOrderWareIn>> input;
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
                    System.out.println("Enter number of warehouses: ");
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
                    Map<String, QueueTableIterator> tablesInMem = DataLoadUtils.mapTablesFromDisk(tables, metadata.entityHandlerMap());
                    DataLoadUtils.ingestData(tablesInMem);
                    break;
                case "3":
                    System.out.println("Option 3: \"Create workload\" selected.");
                    System.out.println("Enter number of warehouses: ");
                    numWare = Integer.parseInt(scanner.nextLine());
                    System.out.println("Allow multi warehouse transactions? [0/1]");
                    boolean multiWarehouses = Integer.parseInt(scanner.nextLine()) > 0;
                    System.out.println("Enter number of transactions per warehouse: ");
                    int numTxn = Integer.parseInt(scanner.nextLine());
                    WorkloadUtils.createWorkload(numWare, numTxn, multiWarehouses);
                    break;
                case "4":
                    System.out.println("Option 4: \"Submit workload\" selected.");
                    System.out.print("Enter duration (ms): [0 for default to 10s] ");
                    int runTime = Integer.parseInt(scanner.nextLine());
                    if(runTime == 0) runTime = 10000;
                    System.out.println("Enter warm up period (ms): [0 for default to 2s] ");
                    int warmUp = Integer.parseInt(scanner.nextLine());
                    if(warmUp == 0) warmUp = 2000;

                    if(numWare == 0){
                        // get number of input files
                        numWare = WorkloadUtils.getNumWorkloadInputFiles();
                        if(numWare == 0){
                            // some unknown bug....
                            System.out.println("Zero warehouses identified. Falling back to warehouse table information...");
                            // fallback to table information
                            numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");
                        }
                        if(numWare == 0){
                            System.out.println("No warehouses identified! Maybe you forgot to generate?");
                            break;
                        }
                        System.out.println(numWare+" warehouses identified");
                    }
                    // reload iterators
                    input = WorkloadUtils.mapWorkloadInputFiles(numWare);

                    // load coordinator
                    if(coordinator == null){
                        coordinator = ExperimentUtils.loadCoordinator(PROPERTIES);
                        // wait for all starter VMSes to connect
                        int numConnected;
                        do {
                            numConnected = coordinator.getConnectedVMSs().size();
                        } while (numConnected < 3);
                    }
                    // TODO implement the gather in network send
                    var expStats = ExperimentUtils.runExperiment(coordinator, input, runTime, warmUp);
                    ExperimentUtils.writeResultsToFile(numWare, expStats, runTime, warmUp,
                            coordinator.getOptions().getNumTransactionWorkers(), coordinator.getOptions().getBatchWindow(), coordinator.getOptions().getMaxTransactionsPerBatch());
                    break;
                case "0":
                    System.out.println("Exiting the application...");
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
