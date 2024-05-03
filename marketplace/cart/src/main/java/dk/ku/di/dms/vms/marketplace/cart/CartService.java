package dk.ku.di.dms.vms.marketplace.cart;

import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.cart.entities.ProductReplica;
import dk.ku.di.dms.vms.marketplace.cart.repositories.ICartItemRepository;
import dk.ku.di.dms.vms.marketplace.cart.repositories.IProductReplicaRepository;
import dk.ku.di.dms.vms.marketplace.common.events.PriceUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
import dk.ku.di.dms.vms.modb.api.annotations.*;

import java.util.Date;
import java.util.List;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("cart")
public final class CartService {

    private final ICartItemRepository cartItemRepository;
    private final IProductReplicaRepository productReplicaRepository;

    public CartService(ICartItemRepository cartItemRepository,
                       IProductReplicaRepository productReplicaRepository) {
        this.cartItemRepository = cartItemRepository;
        this.productReplicaRepository = productReplicaRepository;
    }

    @Inbound(values = {CUSTOMER_CHECKOUT})
    @Outbound(RESERVE_STOCK)
    @Transactional(type=RW)
    @PartitionBy(clazz = CustomerCheckout.class, method = "getId")
    public ReserveStock checkout(CustomerCheckout checkout) {
        System.out.println("Cart received a checkout request with TID: "+checkout.instanceId);

        // get cart items from a customer
        List<CartItem> cartItems = this.cartItemRepository.getCartItemsByCustomerId(checkout.CustomerId);

        for(var cartItem : cartItems){
            var product = this.productReplicaRepository.lookupByKey(new ProductReplica.ProductId(cartItem.seller_id, cartItem.product_id));
            if(product != null && cartItem.version.contentEquals(product.version) && cartItem.unit_price < product.price){
                cartItem.voucher += (product.price - cartItem.unit_price);
                cartItem.unit_price = product.price;
            }
        }

        this.cartItemRepository.deleteAll(cartItems);

        return new ReserveStock(new Date(), checkout, convertCartItems( cartItems ), checkout.instanceId);
    }

    private static List<dk.ku.di.dms.vms.marketplace.common.entities.CartItem> convertCartItems(List<CartItem> cartItems){
        return cartItems.stream().map(f-> new dk.ku.di.dms.vms.marketplace.common.entities.CartItem( f.seller_id, f.product_id, f.product_name, f.unit_price, f.freight_value, f.quantity, f.voucher, f.version)).toList();
    }

    @Inbound(values = {PRICE_UPDATED})
    @Transactional(type=RW)
    @PartitionBy(clazz = PriceUpdated.class, method = "getId")
    public void updateProductPrice(PriceUpdated priceUpdated) {
        System.out.println("Cart received an update price event with version: "+priceUpdated.instanceId);

        // could use issue statement for faster update
        ProductReplica product = this.productReplicaRepository.lookupByKey(
                new ProductReplica.ProductId(priceUpdated.sellerId, priceUpdated.productId));

        if(product == null){
            System.out.println("Cart has no product replica with seller ID "+priceUpdated.sellerId+" : product ID "+priceUpdated.productId);
            return;
        }

        if(product.version.contentEquals(priceUpdated.instanceId)){
            product.price = priceUpdated.price;
        }

        // update all carts?

        this.productReplicaRepository.update(product);
    }

    @Inbound(values = {PRODUCT_UPDATED})
    @Transactional(type=RW)
    @PartitionBy(clazz = ProductUpdated.class, method = "getId")
    public void processProductUpdate(ProductUpdated productUpdated) {
        System.out.println("Cart received a product update event with version: "+productUpdated.version);

        var product = new ProductReplica(productUpdated.seller_id, productUpdated.product_id, productUpdated.name, productUpdated.sku, productUpdated.category,
                productUpdated.description, productUpdated.price, productUpdated.freight_value, productUpdated.status, productUpdated.version);

        this.productReplicaRepository.upsert(product);
    }

}
