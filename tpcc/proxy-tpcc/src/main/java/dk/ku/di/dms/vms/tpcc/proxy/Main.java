package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtilsJdk;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.QueueTableIterator;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;

import java.io.IOException;
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
                    DataLoadUtilsJdk.ingestData(tablesInMem);
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
                    int batchWindow = Integer.parseInt( PROPERTIES.getProperty("batch_window_ms") );
                    int runTime;
                    while(true) {
                        System.out.print("Enter duration (ms): [press 0 for 10s] ");
                        runTime = Integer.parseInt(scanner.nextLine());
                        if (runTime == 0) runTime = 10000;
                        if(runTime < (batchWindow*2)){
                            System.out.print("Duration must be at least 2 * "+batchWindow+" (ms)");
                            continue;
                        }
                        break;
                    }
                    int warmUp;
                    while(true) {
                        System.out.println("Enter warm up period (ms): [press 0 for 2s] ");
                        warmUp = Integer.parseInt(scanner.nextLine());
                        if (warmUp == 0) warmUp = 2000;
                        if(warmUp >= runTime){
                            System.out.print("Warm up must be lower than run time "+runTime+" (ms)");
                            continue;
                        }
                        break;
                    }

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
                    var expStats = ExperimentUtils.runExperiment(coordinator, input, runTime, warmUp);
                    ExperimentUtils.writeResultsToFile(numWare, expStats, runTime, warmUp,
                            coordinator.getOptions().getNumTransactionWorkers(), coordinator.getOptions().getBatchWindow(), coordinator.getOptions().getMaxTransactionsPerBatch());
                    break;
                case "5":
                    System.out.println("Option 5: \"Reset service states\" selected.");
                    // has to wait for all submitted transactions to commit in order to send the reset
                    if(coordinator != null){
                        long numTIDsCommitted = coordinator.getNumTIDsCommitted();
                        long numTIDsSubmitted = coordinator.getNumTIDsSubmitted();
                        if(numTIDsCommitted != numTIDsSubmitted){
                            System.out.println("There are ongoing batches executing! Cannot reset states now. \n Number of TIDs committed: "+numTIDsCommitted+"\n Number of TIDs submitted: "+numTIDsSubmitted);
                            break;
                        }
                    }
                    // cleanup service states
                    for(var vms : TPCcConstants.VMS_TO_PORT_MAP.entrySet()){
                        String host = PROPERTIES.getProperty(vms.getKey() + "_host");
                        try(var client = new MinimalHttpClient(host, vms.getValue())){
                            if(client.sendRequest("PATCH", "", "reset") != 200){
                                System.out.println("Error on resetting "+vms+" state!");
                            }
                        } catch (IOException e) {
                            System.out.println("Exception on resetting "+vms+" state: \n"+e);
                        }
                    }
                    System.out.println("Service states reset.");
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
        System.exit(0);
    }

    private static void printMenu() {
        System.out.println("\n=== Main Menu ===");
        System.out.println("1. Create tables in disk");
        System.out.println("2. Load services with data from tables in disk");
        System.out.println("3. Create workload");
        System.out.println("4. Submit workload");
        System.out.println("5. Reset service states");
        System.out.println("0. Exit");
    }

}
