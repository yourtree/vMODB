package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.Properties;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class TPCcTest {

    private static final int NUM_WARE = 1;

    private static final Properties PROPERTIES = ConfigUtils.loadProperties();

    private static final VmsApplication WAREHOUSE_SVC;
    private static final VmsApplication INVENTORY_SVC;
    private static final VmsApplication ORDER_SVC;

    private static final StorageUtils.EntityMetadata METADATA;

    static {
        try {
            METADATA = StorageUtils.loadEntityMetadata();
            WAREHOUSE_SVC = dk.ku.di.dms.vms.tpcc.warehouse.Main.build();
            INVENTORY_SVC = dk.ku.di.dms.vms.tpcc.inventory.Main.build();
            ORDER_SVC = dk.ku.di.dms.vms.tpcc.order.Main.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void startTPCcServices(){
        WAREHOUSE_SVC.start();
        INVENTORY_SVC.start();
        ORDER_SVC.start();
    }

    @AfterClass
    public static void closeTPCcServices(){
        ORDER_SVC.close();
        INVENTORY_SVC.close();
        WAREHOUSE_SVC.close();
    }

    // @Test
    public void test_A_create_data_and_workload() {
        StorageUtils.createTables(METADATA, NUM_WARE);
        WorkloadUtils.createWorkload(1, 100000);
    }

    @Test
    public void test_B_load_and_ingest() {
        var tableToIndexMap = StorageUtils.mapTablesInDisk(METADATA, NUM_WARE);
        var tableInputMap = DataLoadUtils.loadTablesInMemory(tableToIndexMap, METADATA.entityHandlerMap());
        DataLoadUtils.ingestData(tableInputMap);
        // TODO http get some items and assert not null
    }

    @Test
    public void test_C_submit_workload() throws IOException {
        var input = WorkloadUtils.loadWorkloadData();

        Coordinator coordinator = ExperimentUtils.loadCoordinator(PROPERTIES);
        int numConnected;
        do {
            numConnected = coordinator.getConnectedVMSs().size();
        } while (numConnected < 3);

        var expStats = ExperimentUtils.runExperiment(coordinator, input, 1, 10000, 1000);

        ExperimentUtils.writeResultsToFile(1, expStats, 1, 10000, 1000);

        String host = PROPERTIES.getProperty("warehouse_host");
        int port = TPCcConstants.VMS_TO_PORT_MAP.get("warehouse");

        try(MinimalHttpClient httpClient = new MinimalHttpClient(host, port)){
            String resp1 = httpClient.sendGetRequest("warehouse/1");
            System.out.println(resp1);
            for(int i=1;i<=10;i++) {
                String resp2 = httpClient.sendGetRequest("district/"+i+"/1");
                System.out.println(resp2);
            }
        }


        Assert.assertTrue(true);
    }

}
