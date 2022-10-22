package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.e_commerce.common.entity.Item;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderItemResponse;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.util.List;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum.EQUALS;

@Microservice("cart")
public class CartService {

    private static final SelectStatement BASE_QUERY =
            QueryBuilderFactory.select().select( "item.id, item.quantity, item.price" )
                .from( "cart, cart_items, item" )
                .where( "cart.id", EQUALS, "cart_items.cart_id" )
                .and( "item.id", EQUALS, "cart_items.item_id" ).build();

    private final ICartRepository cartRepository;

    private final ICartItemRepository cartItemRepository;

    public CartService(ICartRepository cartRepository, ICartItemRepository cartItemRepository){
        this.cartRepository = cartRepository;
        this.cartItemRepository = cartItemRepository;
    }

    @Inbound(values = {"new-order-cart"})
    @Outbound("new-order-item-resource")
    @Transactional(type = RW)
    public NewOrderItemResponse newOrder(Long customerId){

        List<CartItem> cartItems = cartItemRepository.findOpenCartByCustomerId(customerId);

        Cart cart = cartRepository.findByCustomerId(customerId);
        cart.sealed = true;

        cartRepository.update(cart);

        return new NewOrderItemResponse(cartItems.stream()
                .map( ci -> new Item( ci.productId, ci.quantity, ci.unitPrice ))
                .collect(Collectors.toList()));

    }

}
