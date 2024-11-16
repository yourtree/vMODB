package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoader;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import org.junit.Test;

public final class AppTest {
    @Test
    public void test() throws Exception {
        StorageUtils.EntityMetadata metadata = StorageUtils.loadEntityMetadata();
        var tableToIndexMap = StorageUtils.loadTables(metadata);

        // init test service
        new TestService().run();

        // ingest data in warehouse
        DataLoader.load(tableToIndexMap, metadata.entityHandlerMap());

        // query

    }
}
