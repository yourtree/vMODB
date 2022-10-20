package dk.ku.di.dms.vms.e_commerce.order;

import dk.ku.di.dms.vms.e_commerce.common.entity.Item;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderItemResource;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderResult;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderUserResource;
import dk.ku.di.dms.vms.e_commerce.common.events.PaymentResponse;
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

    @Inbound(values = {"new-order-item-resource","new-order-user-resource","payment-response"})
    @Outbound("new-order-result")
    @Transactional(type = RW)
    public NewOrderResult newOrder(NewOrderItemResource newOrderItemResource, NewOrderUserResource newOrderUserResource, PaymentResponse paymentResponse){ // can save this as a json

        if(!paymentResponse.authorised) return null;

        // to get the id from the MODB
        Order order = orderRepository.insertAndGet( new Order( newOrderUserResource.customer,  newOrderUserResource.card,  newOrderUserResource.address, paymentResponse.debitAmount ) );

        for(Item item : newOrderItemResource.items){
            orderItemRepository.insert( new OrderItem( item.quantity, item.unitPrice, order.id ) );
        }



        // create shipment event... actually create a new order... other vmss will listen to that, like the shipment and recommender engine

        return null;

    }

}
