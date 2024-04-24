package dk.ku.di.dms.vms.marketplace.product;

import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.TransactionMark;
import dk.ku.di.dms.vms.marketplace.common.events.UpdatePrice;
import dk.ku.di.dms.vms.modb.api.annotations.*;

import static dk.ku.di.dms.vms.marketplace.common.Constants.PRODUCT_UPDATED;
import static dk.ku.di.dms.vms.marketplace.common.Constants.UPDATE_PRICE;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("product")
public final class ProductService {

    private final IProductRepository productRepository;

    public ProductService(IProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @Inbound(values = {"update_product"})
    @Outbound(PRODUCT_UPDATED)
    @Transactional(type=W)
    @PartitionBy(clazz = UpdateProductEvent.class, method = "getId")
    public ProductUpdated updateProduct(UpdateProductEvent updateEvent) {
        System.out.println("Product received an product update event with TID: "+updateEvent.version);

        // can use issue statement for faster update
        Product product = new Product(updateEvent.seller_id, updateEvent.product_id, updateEvent.name, updateEvent.sku, updateEvent.category,
                updateEvent.description, updateEvent.price, updateEvent.freight_value, updateEvent.status, updateEvent.version);

        this.productRepository.update(product);

        return new ProductUpdated( updateEvent.seller_id, updateEvent.product_id, updateEvent.version);
    }

    @Inbound(values = {UPDATE_PRICE})
    @Outbound("transaction_mark")
    @Transactional(type=W)
    @PartitionBy(clazz = UpdatePrice.class, method = "getId")
    public TransactionMark updateProductPrice(UpdatePrice updatePriceEvent) {
        System.out.println("Stock received an update price event with TID: "+updatePriceEvent.instanceId);

        // could use issue statement for faster update
        Product product = this.productRepository.lookupByKey(new Product.ProductId(updatePriceEvent.sellerId, updatePriceEvent.productId));

        product.version = updatePriceEvent.instanceId;
        product.price = updatePriceEvent.price;

        this.productRepository.update(product);

        return new TransactionMark( updatePriceEvent.instanceId, TransactionMark.TransactionType.PRICE_UPDATE,
                updatePriceEvent.sellerId, TransactionMark.MarkStatus.SUCCESS, "product");
    }

}
