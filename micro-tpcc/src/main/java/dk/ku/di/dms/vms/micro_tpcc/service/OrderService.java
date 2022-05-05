package dk.ku.di.dms.vms.micro_tpcc.service;

import dk.ku.di.dms.vms.micro_tpcc.events.*;
import dk.ku.di.dms.vms.sdk.core.annotations.Inbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Microservice;
import dk.ku.di.dms.vms.sdk.core.annotations.Terminal;
import dk.ku.di.dms.vms.sdk.core.annotations.Transactional;
import dk.ku.di.dms.vms.micro_tpcc.entity.NewOrder;
import dk.ku.di.dms.vms.micro_tpcc.entity.Order;
import dk.ku.di.dms.vms.micro_tpcc.entity.OrderLine;
import dk.ku.di.dms.vms.micro_tpcc.repository.order.INewOrderRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.order.IOrderLineRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.order.IOrderRepository;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

@Microservice("order")
public class OrderService {

    private final IOrderRepository orderRepository;
    private final INewOrderRepository newOrderRepository;
    private final IOrderLineRepository orderLineRepository;

    public OrderService(IOrderRepository orderRepository,
                        INewOrderRepository newOrderRepository,
                        IOrderLineRepository orderLineRepository){
        this.orderRepository = orderRepository;
        this.newOrderRepository = newOrderRepository;
        this.orderLineRepository = orderLineRepository;
    }

    // need all values from waredist out
    @Inbound(values = {"customer-new-order-out","stock-new-order-out","stock-new-order-in",
            "items-new-order-out","waredist-new-order-out"})
    @Transactional
    @Terminal
    public void processNewOrderItemsAsync(CustomerNewOrderOut customerNewOrderOut,
                                          StockNewOrderOut stockNewOrderOut,
                                          StockNewOrderIn stockNewOrderIn,
                                          ItemNewOrderOut itemsNewOrderOut,
                                          WareDistNewOrderOut wareDistNewOrderOut){
        int n = stockNewOrderIn.ol_cnt();

        CompletableFuture<?>[] futures = new CompletableFuture[n+1];

        // the developer should be sure about the semantics
        futures[n] = CompletableFuture.runAsync(() -> {

            Order orderToInsert = new Order(
                    wareDistNewOrderOut.d_next_o_id(),
                    wareDistNewOrderOut.d_id(),
                    wareDistNewOrderOut.d_w_id(),
                    customerNewOrderOut.c_id(),
                    new Date(),
                    0, // FIXME
                    n,  // <====== ol_count is necessary from the number of item list
                    1 // FIXME
            );

            orderRepository.insert(orderToInsert);

            NewOrder newOrderToInsert = new NewOrder(
                    wareDistNewOrderOut.d_next_o_id(),
                    wareDistNewOrderOut.d_id(),
                    wareDistNewOrderOut.d_w_id()
            );

            newOrderRepository.insert(newOrderToInsert);

        });

        for(int i = 0; i < n; i++){

            int finalI = i;
            int itemId = stockNewOrderIn.itemsIds()[finalI];
            int wareId = stockNewOrderIn.supware()[finalI];
            futures[i] = CompletableFuture.runAsync(() -> {

                float ol_amount = stockNewOrderIn.quantity()[finalI] *
                        itemsNewOrderOut.itemsIds()[finalI] *
                        (1 + wareDistNewOrderOut.w_tax() + wareDistNewOrderOut.d_tax()) *
                        (1 - customerNewOrderOut.c_discount());

                OrderLine ol = new OrderLine();
                ol.ol_o_id = wareDistNewOrderOut.d_next_o_id();
                ol.ol_d_id = wareDistNewOrderOut.d_id();
                ol.ol_w_id = wareDistNewOrderOut.d_w_id();
                ol.ol_amount = ol_amount;
                ol.ol_dist_info = stockNewOrderOut.itemsDistInfo()[finalI];
                ol.ol_number = finalI+1;
                ol.ol_quantity = n;
                ol.ol_i_id = itemId;
                ol.ol_supply_w_id = wareId;

                orderLineRepository.insert(ol);

            });

        }

        CompletableFuture.allOf(futures).join();

    }


}
