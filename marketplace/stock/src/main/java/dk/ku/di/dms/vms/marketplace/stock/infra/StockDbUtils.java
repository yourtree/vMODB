package dk.ku.di.dms.vms.marketplace.stock.infra;

import dk.ku.di.dms.vms.marketplace.stock.StockItem;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

public final class StockDbUtils {

    private static final IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();

    /**
     * Bypass the transaction manager safely on data ingestion
     */
    public static void addStockItem(String payload, AbstractProxyRepository<StockItem.StockId, StockItem> repository, Table table){
        StockItem stockItem = deserializeStockItem(payload);
        Object[] obj = repository.extractFieldValuesFromEntityObject(stockItem);
        IKey key = KeyUtils.buildRecordKey( table.schema().getPrimaryKeyColumns(), obj );
        table.underlyingPrimaryKeyIndex().insert(key, obj);
    }

    public static StockItem deserializeStockItem(String payload) {
        return SERDES.deserialize(payload, StockItem.class);
    }


}
