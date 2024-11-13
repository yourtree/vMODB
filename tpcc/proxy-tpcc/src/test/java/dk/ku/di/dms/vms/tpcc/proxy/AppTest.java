package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import org.junit.Test;

public class AppTest
{
    @Test
    public void test() throws Exception {
        StorageUtils.createTables();
    }
}
