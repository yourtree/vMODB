package dk.ku.di.dms.vms.e_commerce.order;

import dk.ku.di.dms.vms.e_commerce.common.entity.Item;
import dk.ku.di.dms.vms.e_commerce.common.events.*;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("order")
public class OrderService {

    private final IOrderRepository orderRepository;

    private final IOrderItemRepository orderItemRepository;

    public OrderService(IOrderRepository orderRepository,
                        IOrderItemRepository itemRepository) {
        this.orderRepository = orderRepository;
        this.orderItemRepository = itemRepository;
    }

    @Inbound(values = {"new-order-item-resource","new-order-user-resource"})
    @Outbound("payment-request")
    @Transactional(type = RW)
    public PaymentRequest newOrder(NewOrderItemResponse newOrderItemResource,
                                   NewOrderUserResponse newOrderUserResource){

        // TODO maybe also discounts coupons a user has?

        float totalPrice = 0;

        // calculate total amount
        for(Item item : newOrderItemResource.items){
            totalPrice += item.unitPrice;
        }

        Order order_ = new Order( newOrderUserResource.customer, totalPrice );
        order_.orderStatus = "IN_PROGRESS";

        // to get the id from the database, sequence constraint sequential id provided by design
        Order order = orderRepository.insertAndGet( order_ );

        // can be made parallel by application code, not unsafe for correctness
        for(Item item : newOrderItemResource.items)
            orderItemRepository.insert( new OrderItem( item.quantity, item.unitPrice, order.id ) );

        // create payment request
        return new PaymentRequest( order.id, totalPrice, newOrderUserResource.customer );

    }

}
