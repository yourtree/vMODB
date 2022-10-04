package dk.ku.di.dms.vms.modb.persistence;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Catalog;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.query.TestCommon;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import org.junit.Test;

import java.nio.ByteBuffer;

public class BulkLoadTest {

    @Test
    public void overflowTest(){

        Catalog catalog = TestCommon.getDefaultCatalog();
        Table table = catalog.getTable("item");

        int maxItems = 20;
        int bufferSize = table.getSchema().getRecordSizeWithoutHeader() * maxItems;
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer( bufferSize );

        for(int i = 1; i <= maxItems; i++){
            produceItem(buffer, i);
        }

        TransactionFacade.bulkInsert(table, buffer, maxItems);

        long resAddress = table.primaryKeyIndex().retrieve(SimpleKey.of(7));

        var unsafe = MemoryUtils.UNSAFE;
        assert unsafe.getInt(resAddress + Schema.recordHeader) == 7;
        assert unsafe.getFloat( resAddress + Schema.recordHeader + Integer.BYTES ) == 7f;

    }

    @Test
    public void test(){

        Catalog catalog = TestCommon.getDefaultCatalog();
        Table table = catalog.getTable("item");
//        String[] itemColumns = { "i_id", "i_price", "i_name", "i_data" };
        int maxItems = 10;
        int bufferSize = table.getSchema().getRecordSizeWithoutHeader() * maxItems;
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer( bufferSize );

        for(int i = 1; i <= maxItems; i++){
            produceItem(buffer, i);
        }

        TransactionFacade.bulkInsert(table, buffer, maxItems);

        long resAddress = table.primaryKeyIndex().retrieve(SimpleKey.of(7));

        var unsafe = MemoryUtils.UNSAFE;
        assert unsafe.getInt(resAddress + Schema.recordHeader) == 7;
        assert unsafe.getFloat( resAddress + Schema.recordHeader + Integer.BYTES ) == 7f;

    }

    private void produceItem(ByteBuffer buffer, int i){
        buffer.putInt(i);
        buffer.putFloat(i);
        /*
         * TODO would be good embrace length of string attributes
         * {@link javax.persistence.Column#length()}
          */
        DataTypeUtils.callWriteFunction(buffer, DataType.STRING, String.valueOf(i));
        DataTypeUtils.callWriteFunction(buffer, DataType.STRING, String.valueOf(i));
    }

}
