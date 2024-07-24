package dk.ku.di.dms.vms.marketplace;


import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

public class CheckpointingTest extends CartProductWorkflowTest {

    private static VmsApplication PRODUCT_VMS;

    @BeforeClass
    public static void setUpBeforeClass() {
        // System.setProperty("logging", "true");
        System.setProperty("checkpointing", "true");
        PRODUCT_VMS = dk.ku.di.dms.vms.marketplace.product.Main.init();
    }

    @Override
    protected void initCartAndProduct() {
        dk.ku.di.dms.vms.marketplace.cart.Main.main(null);
    }

    /**
     * Check whether state has been checkpointed
     */
    @Override
    protected void additionalAssertions() {
        String userHome = ConfigUtils.getUserHome();
        String filePath = userHome + "/vms/products.data";
        Path path = Paths.get(filePath);
        Schema productSchema = PRODUCT_VMS.getSchema("products");
        try {
            FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
            var bb = MemoryManager.getTemporaryDirectBuffer( (int)fc.size() );
            //bb.order(ByteOrder.nativeOrder());
            int res = fc.read(bb);
            Assert.assertEquals(4260, res);
            /* this is not working well...
            bb.position(0);
            Set<Integer> setOfIds = new HashSet<>();
            // read records from the byte buffer
            for(int i = 1; i <= MAX_ITEMS; i++){
                int pos = 0;
                bb.position(bb.position() + Schema.RECORD_HEADER);
                Object[] record = new Object[productSchema.columnDataTypes().length];
                for(DataType dt : productSchema.columnDataTypes()) {
                    var rf = DataTypeUtils.getReadFunction(dt);
                    record[pos] = rf.apply(bb);
                    pos++;
                }
                setOfIds.add((Integer) record[0]);
            }
            Assert.assertEquals(MAX_ITEMS, setOfIds.size());
             */
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
