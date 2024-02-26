package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import org.junit.Test;

import static dk.ku.di.dms.vms.marketplace.common.Constants.PRODUCT_UPDATED;
import static java.lang.Thread.sleep;

public class StockTest {

    private static VmsApplication getVmsApplication() throws Exception {
        return VmsApplication.build("localhost", Constants.STOCK_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.stock",
                "dk.ku.di.dms.vms.marketplace.common"
        });
    }

    /**
     *  Add sellers first to avoid foreign key constraint violation
     */
    @SuppressWarnings("unchecked")
    private static void insertStockItems(VmsApplication vms) {
        var stockTable = vms.getTable("stock_items");
        var stockRepository = (AbstractProxyRepository<StockItem.StockId, StockItem>) vms.getRepositoryProxy("stock_items");

        for(int i = 1; i <= 10; i++){
            var stock = new StockItem( i, 1, 100000, 0, 0, 0,  "test", "1" );
            Object[] obj = stockRepository.extractFieldValuesFromEntityObject(stock);
            IKey key = KeyUtils.buildRecordKey( stockTable.schema().getPrimaryKeyColumns(), obj );
            stockTable.underlyingPrimaryKeyIndex().insert(key, obj);
        }
    }

    private static void generateProductUpdated(int sellerId, int productId, int tid, int previousTid, String instanceId,  VmsApplication vms) {
        ProductUpdated productUpdated = new ProductUpdated(sellerId, productId, instanceId);
        InboundEvent inboundEvent = new InboundEvent(tid, previousTid, 1,
                PRODUCT_UPDATED, ProductUpdated.class, productUpdated);
        vms.internalChannels().transactionInputQueue().add(inboundEvent);
    }

    private static void generateReserveStock(){
//        ReserveStock reserveStock = new ReserveStock(sellerId, productId, instanceId);
//        InboundEvent inboundEvent = new InboundEvent(tid, previousTid, 1,
//                PRODUCT_UPDATED, ProductUpdated.class, productUpdated);
//        vms.internalChannels().transactionInputQueue().add(inboundEvent);
    }

    @Test
    public void testIndependentPartitionExecution() throws Exception {
        var vms = getVmsApplication();
        vms.start();

        insertStockItems(vms);

        for(int i = 1; i <= 10; i++) {
            generateProductUpdated(i, 1, i, i - 1, String.valueOf(i), vms);
        }

        sleep(2000);

        assert vms.lastTidFinished() == 10;
    }

    /**
     * TODO test the case where some partitioned tasks intersect
     *  in this case, we can allow higher partitioned TIDs to
     *  proceed.
     *  in the scheduler, create a waiting queue of partitioned tasks
     *  if the partition is busy, just keeps triggering the subsequent tasks
     *  caution: cannot allow single thread or parallel to execution
     *  until all partitioned have executed
     */
    @Test
    public void testCollisionPartitionExecution() throws Exception {
        var vms = getVmsApplication();
        vms.start();

        insertStockItems(vms);

        for(int i = 1; i <= 5; i++) {
            generateProductUpdated(i, 1, i, i - 1, String.valueOf(i), vms);
        }
        for(int i = 6; i <= 10; i++) {
            generateProductUpdated(i, 2, i, i - 1, String.valueOf(i), vms);
        }

        sleep(2000);

        assert vms.lastTidFinished() == 10;
    }

    @Test
    public void testMixedSingleThreadPartitionedExecution() throws Exception {

        var vms = getVmsApplication();
        vms.start();

        insertStockItems(vms);



        for(int i = 1; i <= 10; i++) {
            generateProductUpdated(i, 1, i, i - 1, String.valueOf(i), vms);
        }

        sleep(2000);

        assert vms.lastTidFinished() == 10;

    }

}
