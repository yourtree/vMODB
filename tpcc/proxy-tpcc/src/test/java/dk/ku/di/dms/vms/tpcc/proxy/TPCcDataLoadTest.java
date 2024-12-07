package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class TPCcDataLoadTest {

    private static final int NUM_WARE = 2;

    private static final StorageUtils.EntityMetadata METADATA;

    static {
        try {
            METADATA = StorageUtils.loadEntityMetadata();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test_A_create_data() {
        StorageUtils.createTables(METADATA, NUM_WARE);
    }

    @Test
    public void test_B_create_workload() {
        // 2.5M~483 MB - 5M~966MB
        WorkloadUtils.createWorkload(NUM_WARE, 5000000);
    }

}
