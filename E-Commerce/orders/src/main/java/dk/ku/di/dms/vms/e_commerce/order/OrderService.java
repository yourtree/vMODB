package dk.ku.di.dms.vms.e_commerce.order;

import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderItemResource;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderResult;
import dk.ku.di.dms.vms.e_commerce.common.events.PaymentRequest;
import dk.ku.di.dms.vms.e_commerce.common.events.PaymentResponse;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;

@Microservice("order")
public class OrderService {

    private final IOrderRepository orderRepository;

    public OrderService(IOrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    // maybe also make sure discounts are applied correctly, that means receiving the discount input.
    // if discount is no longer applied then must process the correct value or this the job of the cart?
    // usually this is the case. the cart must make sure the discount is applied correctly
    // discount microservice can receive some items, apply the discounts and then create the
    // responsible for matching the new product prices...?
    public PaymentRequest newOrder(NewOrderItemResource newOrderItemResource){ // can save this as a json

        // create shipment... actually create a new order... other vmss will listen to that, like the shipment and recommender engine

        return null;
    }

    public NewOrderResult processPaymentResponse(PaymentResponse paymentResponse){

        // and then request the stock to process the items.. or we can as the tpcc does.. or we can create refill stock event... many options on how to model the benchmark
        // how does apiary is doing?

        return null;
    }

}
