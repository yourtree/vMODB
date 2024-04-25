package dk.ku.di.dms.vms.marketplace.product;

import dk.ku.di.dms.vms.marketplace.common.events.PriceUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdatePrice;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdateProduct;
import dk.ku.di.dms.vms.modb.api.annotations.*;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("product")
public final class ProductService {

    private final IProductRepository productRepository;

    public ProductService(IProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @Inbound(values = {UPDATE_PRODUCT})
    @Outbound(PRODUCT_UPDATED)
    @Transactional(type=W)
    @PartitionBy(clazz = UpdateProduct.class, method = "getId")
    public ProductUpdated updateProduct(UpdateProduct updateEvent) {
        System.out.println("Product received a product update event with TID: "+updateEvent.version);

        // can use issue statement for faster update
        Product product = new Product(updateEvent.seller_id, updateEvent.product_id, updateEvent.name, updateEvent.sku, updateEvent.category,
                updateEvent.description, updateEvent.price, updateEvent.freight_value, updateEvent.status, updateEvent.version);

        this.productRepository.update(product);

        return new ProductUpdated( updateEvent.seller_id, updateEvent.product_id, updateEvent.name, updateEvent.sku, updateEvent.category, updateEvent.description, updateEvent.price, updateEvent.freight_value, updateEvent.status, updateEvent.version);
    }

    @Inbound(values = {UPDATE_PRICE})
    @Outbound(PRICE_UPDATED)
    @Transactional(type=RW)
    @PartitionBy(clazz = UpdatePrice.class, method = "getId")
    public PriceUpdated updateProductPrice(UpdatePrice updatePrice) {
        System.out.println("Product received an update price event with TID: "+updatePrice.instanceId);

        // could use issue statement for faster update
        Product product = this.productRepository.lookupByKey(new Product.ProductId(updatePrice.sellerId, updatePrice.productId));

        product.version = updatePrice.instanceId;
        product.price = updatePrice.price;

        this.productRepository.update(product);

        return new PriceUpdated(updatePrice.sellerId, updatePrice.productId, updatePrice.price, updatePrice.instanceId);
    }

}
