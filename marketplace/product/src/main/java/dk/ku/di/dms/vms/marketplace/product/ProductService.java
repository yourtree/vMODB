package dk.ku.di.dms.vms.marketplace.product;

import dk.ku.di.dms.vms.marketplace.common.ProductUpdatedEvent;
import dk.ku.di.dms.vms.marketplace.common.TransactionMark;
import dk.ku.di.dms.vms.marketplace.common.UpdatePriceEvent;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("product")
public class ProductService {

    private final IProductRepository productRepository;


    public ProductService(IProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @Inbound(values = {"update_product"})
    @Outbound("product_updated")
    @Transactional(type=W)
    public ProductUpdatedEvent updateProduct(UpdateProductEvent updateEvent) {
        System.out.println("Product received an product update event");

        // can use issue statement for faster update
        Product product = new Product(updateEvent.seller_id, updateEvent.product_id, updateEvent.name, updateEvent.sku, updateEvent.category,
                updateEvent.description, updateEvent.price, updateEvent.freight_value, updateEvent.status, updateEvent.version);

        productRepository.update(product);

        return new ProductUpdatedEvent( updateEvent.seller_id, updateEvent.product_id, updateEvent.version);
    }

    @Inbound(values = {"update_price"})
    @Outbound("transaction_mark")
    @Transactional(type=W)
    public TransactionMark updateProductPrice(UpdatePriceEvent updatePriceEvent) {
        System.out.println("I am alive. The scheduler has scheduled me successfully!");

        // can use issue statement for faster update

        Product product = productRepository.lookupByKey(new Product.ProductId(updatePriceEvent.sellerId, updatePriceEvent.productId));

        product.version = updatePriceEvent.instanceId;
        product.price = updatePriceEvent.price;

        productRepository.update(product);

        return new TransactionMark( updatePriceEvent.instanceId, TransactionMark.TransactionType.PRICE_UPDATE,
                updatePriceEvent.sellerId, TransactionMark.MarkStatus.SUCCESS, "product");
    }

}
