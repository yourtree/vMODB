package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoader;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

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
        WAREHOUSE_SVC.close();
        INVENTORY_SVC.close();
        ORDER_SVC.close();
    }

    //@Test
    public void test_A_create_data_and_workload() {
        StorageUtils.createTables(METADATA, NUM_WARE);
        WorkloadUtils.createWorkload(1, 100000);
    }

    @Test
    public void test_B_load_and_ingest() {
        var tableToIndexMap = StorageUtils.loadTables(METADATA, NUM_WARE);
        boolean res = DataLoader.load(tableToIndexMap, METADATA.entityHandlerMap(), 1);
        Assert.assertTrue(res);
    }

    @Test
    public void test_C_submit_workload() {
        var input = WorkloadUtils.loadWorkloadData();

        Coordinator coordinator = WorkloadUtils.loadCoordinator(PROPERTIES);
        int numConnected;
        do {
            numConnected = coordinator.getConnectedVMSs().size();
        } while (numConnected < 3);

        var res = WorkloadUtils.runExperiment(coordinator, input, 1, 1000);

        Assert.assertTrue(res.perc() > 0);
    }

}
