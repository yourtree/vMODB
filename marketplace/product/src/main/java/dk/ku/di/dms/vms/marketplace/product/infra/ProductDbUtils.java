package dk.ku.di.dms.vms.marketplace.product.infra;

import dk.ku.di.dms.vms.marketplace.product.Product;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

import java.util.Date;

public final class ProductDbUtils {

    private static final IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();

    public static void addProduct(String str, AbstractProxyRepository<Product.ProductId, Product> repository, Table table){
        Product product = SERDES.deserialize(str, Product.class);
        Object[] obj = repository.extractFieldValuesFromEntityObject(product);
        IKey key = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), obj);

        // created and updated at
        Date dt = new Date();
        obj[obj.length - 1] = dt;
        obj[obj.length - 2] = dt;
        table.underlyingPrimaryKeyIndex().insert(key, obj);
    }

}
