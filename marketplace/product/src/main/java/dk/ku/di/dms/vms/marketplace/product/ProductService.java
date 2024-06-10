package dk.ku.di.dms.vms.marketplace.product;

import dk.ku.di.dms.vms.marketplace.common.events.PriceUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdatePrice;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdateProduct;
import dk.ku.di.dms.vms.modb.api.annotations.*;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;
import static java.lang.System.Logger.Level.INFO;

@Microservice("product")
public final class ProductService {

    private static final System.Logger LOGGER = System.getLogger(ProductService.class.getName());

    private final IProductRepository productRepository;

    public ProductService(IProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @Inbound(values = {UPDATE_PRODUCT})
    @Outbound(PRODUCT_UPDATED)
    @Transactional(type=W)
    @Parallel
    public ProductUpdated updateProduct(UpdateProduct updateEvent) {
        LOGGER.log(INFO,"APP-"+Thread.currentThread().threadId()+": Product received a product update event with version: "+updateEvent.version);

        // can use issue statement for faster update

        /* only for testing
        @PartitionBy(clazz = UpdateProduct.class, method = "getId")
        Product product = new Product(updateEvent.seller_id, updateEvent.product_id, updateEvent.name, updateEvent.sku, updateEvent.category,
                updateEvent.description, updateEvent.price, updateEvent.freight_value, updateEvent.status, updateEvent.version);

        this.productRepository.update(product);
        */

        return new ProductUpdated( updateEvent.seller_id, updateEvent.product_id, updateEvent.name, updateEvent.sku, updateEvent.category, updateEvent.description, updateEvent.price, updateEvent.freight_value, updateEvent.status, updateEvent.version);
    }

    @Inbound(values = {UPDATE_PRICE})
    @Outbound(PRICE_UPDATED)
    @Transactional(type=RW)
    @PartitionBy(clazz = UpdatePrice.class, method = "getId")
    public PriceUpdated updateProductPrice(UpdatePrice updatePrice) {
        LOGGER.log(INFO,"APP: Product received an update price event with version: "+updatePrice.instanceId);

        // could use issue statement for faster update
        Product product = this.productRepository.lookupByKey(new Product.ProductId(updatePrice.sellerId, updatePrice.productId));

        product.version = updatePrice.instanceId;
        product.price = updatePrice.price;

        this.productRepository.update(product);

        return new PriceUpdated(updatePrice.sellerId, updatePrice.productId, updatePrice.price, updatePrice.instanceId);
    }

}
