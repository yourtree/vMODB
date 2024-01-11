package dk.ku.di.dms.vms.marketplace.order;

import dk.ku.di.dms.vms.marketplace.common.events.StockConfirmed;
import dk.ku.di.dms.vms.marketplace.common.events.TransactionMark;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("order")
public class OrderService {

    private final IOrderRepository orderRepository;


    public OrderService(IOrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Inbound(values = {"stock_confirmed"})
    @Outbound("transaction_mark")
    @Transactional(type=W)
    public TransactionMark processStockConfirmed(StockConfirmed stockConfirmed) {
        System.out.println("Order received a stock confirmed event");

        // TODO finish logic

        return new TransactionMark( stockConfirmed.instanceId, TransactionMark.TransactionType.CUSTOMER_SESSION, 1, TransactionMark.MarkStatus.SUCCESS, "order" );
    }

}
