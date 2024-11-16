package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoader;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import org.junit.Assert;
import org.junit.Test;

public final class AppTest {
    @Test
    public void testLoadAndIngest() throws Exception {
        StorageUtils.EntityMetadata metadata = StorageUtils.loadEntityMetadata();
        var tableToIndexMap = StorageUtils.loadTables(metadata);

        // init test service
        new TestService().run();

        // ingest data in warehouse
        DataLoader.load(tableToIndexMap, metadata.entityHandlerMap());
    }

    @Test
    public void testWorkload() {

        // create
        var created = WorkloadUtils.createWorkload();

        // load
        var loaded = WorkloadUtils.loadWorkloadData();

        Assert.assertEquals(created.size(), loaded.size());

        for(int i = 0; i < created.size(); i++){
            Assert.assertEquals(created.get(i), loaded.get(i));
        }

    }
}
