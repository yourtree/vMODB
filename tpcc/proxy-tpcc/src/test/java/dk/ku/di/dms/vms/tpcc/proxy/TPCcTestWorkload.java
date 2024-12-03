package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.entities.District;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import dk.ku.di.dms.vms.web_common.HttpUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.Properties;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class TPCcTestWorkload {

    private static final int NUM_WARE = 1;

    private static final int RUN_TIME = 20000;

    private static final int WARM_UP = 2000;

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

    @Test
    public void test_B_load_and_ingest() throws IOException {
        var tableToIndexMap = StorageUtils.mapTablesInDisk(METADATA, NUM_WARE);
        var tableInputMap = DataLoadUtils.loadTablesInMemory(tableToIndexMap, METADATA.entityHandlerMap());
        DataLoadUtils.ingestData(tableInputMap);
        hasDataBeenIngested();
    }

    private static final int NUM_WORKERS = 1;

    @Test
    public void test_C_submit_workload() throws IOException {
        var input = WorkloadUtils.loadWorkloadData();

        Coordinator coordinator = ExperimentUtils.loadCoordinator(PROPERTIES);
        int numConnected;
        do {
            numConnected = coordinator.getConnectedVMSs().size();
        } while (numConnected < 3);

        var expStats = ExperimentUtils.runExperiment(coordinator, input, NUM_WORKERS, RUN_TIME, WARM_UP);

        ExperimentUtils.writeResultsToFile(NUM_WARE, expStats, NUM_WORKERS, RUN_TIME, WARM_UP);

        String host = PROPERTIES.getProperty("warehouse_host");
        int port = TPCcConstants.VMS_TO_PORT_MAP.get("warehouse");
        var serdesProxy = VmsSerdesProxyBuilder.build();
        // query get some items and assert correctness
        try(MinimalHttpClient httpClient = new MinimalHttpClient(host, port)){
            //String resp1 = httpClient.sendGetRequest("warehouse/1");
            //System.out.println(resp1);
            for(int i=1;i<=10;i++) {
                String resp2 = httpClient.sendGetRequest("district/"+i+"/1");
                // System.out.println(resp2);
                var parsedResp = HttpUtils.parseRequest(resp2);
                var district = serdesProxy.deserialize(parsedResp.body(), District.class);
                Assert.assertTrue(district.d_next_o_id > 3001);
            }
        }
    }

    private static void hasDataBeenIngested() throws IOException {
        String host = PROPERTIES.getProperty("warehouse_host");
        int port = TPCcConstants.VMS_TO_PORT_MAP.get("warehouse");
        var serdesProxy = VmsSerdesProxyBuilder.build();
        try(MinimalHttpClient httpClient = new MinimalHttpClient(host, port)){
            String resp1 = httpClient.sendGetRequest("warehouse/1");
            var parsedResp1 = HttpUtils.parseRequest(resp1);
            var ware = serdesProxy.deserialize(parsedResp1.body(), Warehouse.class);
            Assert.assertEquals(1, ware.w_id);
            for(int i=1;i<=10;i++) {
                String resp2 = httpClient.sendGetRequest("district/"+i+"/1");
                // System.out.println(resp2);
                var parsedResp2 = HttpUtils.parseRequest(resp2);
                var district = serdesProxy.deserialize(parsedResp2.body(), District.class);
                Assert.assertEquals(3001, district.d_next_o_id);
            }
        }
    }

}
