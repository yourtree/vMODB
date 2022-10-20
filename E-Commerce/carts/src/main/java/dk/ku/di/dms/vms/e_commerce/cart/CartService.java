package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderItemResource;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.builder.SelectStatementBuilder;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("cart")
public class CartService {

    private final ICartRepository cartRepository;

    private final IItemRepository itemRepository;

    public CartService(ICartRepository cartRepository, IItemRepository itemRepository){
        this.cartRepository = cartRepository;
        this.itemRepository = itemRepository;
    }

    @Inbound(values = {"new-order-cart"})
    @Outbound("new-order-item-resource")
    @Transactional(type = RW)
    public NewOrderItemResource newOrder(Long customerId){

        Cart cart = cartRepository.findByCustomerId(customerId);

        SelectStatementBuilder builder = QueryBuilderFactory.select();
        // TODO finish builder.select(  )
        return null;

    }

}
