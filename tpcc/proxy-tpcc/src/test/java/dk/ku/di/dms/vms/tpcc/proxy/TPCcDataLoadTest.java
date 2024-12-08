package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.modb.definition.key.composite.TripleCompositeKey;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class TPCcDataLoadTest {

    private static final int NUM_WARE = 32;

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

    @Test
    public void test_C_test_hash_entry() {
        // generate entries for district for different warehouses, compare the hashes generated. try to find a way to avoid duplicate hashes
        Map<Integer, List<TripleCompositeKey>> map = new HashMap<>();
        for(int w_id = 1; w_id <= NUM_WARE; w_id++){
            for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                for(int c_id = 1; c_id <= TPCcConstants.NUM_CUST_PER_DIST; c_id++) {
                    var key = TripleCompositeKey.of(c_id, d_id, w_id);
                    map.computeIfAbsent(key.hashCode(), ignored -> new ArrayList<>()).add(key);
                    Assert.assertFalse(map.get(key.hashCode()).size() > 1);
                }
            }
        }
    }

}
