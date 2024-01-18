package dk.ku.di.dms.vms.marketplace.order;

import dk.ku.di.dms.vms.marketplace.common.events.InvoiceIssued;
import dk.ku.di.dms.vms.marketplace.common.events.StockConfirmed;
import dk.ku.di.dms.vms.marketplace.order.entities.*;
import dk.ku.di.dms.vms.marketplace.order.repositories.ICustomerOrderRepository;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderHistoryRepository;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderItemRepository;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.*;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.*;

@Microservice("order")
public class OrderService {

    private final IOrderRepository orderRepository;

    private final ICustomerOrderRepository customerOrderRepository;

    private final IOrderItemRepository orderItemRepository;

    private final IOrderHistoryRepository orderHistoryRepository;

    public OrderService(IOrderRepository orderRepository,
                        ICustomerOrderRepository customerOrderRepository,
                        IOrderItemRepository orderItemRepository,
                        IOrderHistoryRepository orderHistoryRepository) {
        this.orderRepository = orderRepository;
        this.customerOrderRepository = customerOrderRepository;
        this.orderItemRepository = orderItemRepository;
        this.orderHistoryRepository = orderHistoryRepository;
    }

    @Inbound(values = {"stock_confirmed"})
    @Outbound("invoice_issued")
    @Transactional(type=RW)
    public InvoiceIssued processStockConfirmed(StockConfirmed stockConfirmed) {
        Date now = new Date();
        System.out.println("Order received a stock confirmed event with TID: "+ stockConfirmed.instanceId);

        // calculate total freight_value
        float total_freight = 0;
        for(var item : stockConfirmed.items)
        {
            total_freight += item.FreightValue;
        }

        float total_amount = 0;
        for(var item : stockConfirmed.items)
        {
            total_amount += (item.UnitPrice * item.Quantity);
        }

        // total before discounts
        float total_items = total_amount;

        // apply vouchers per product, but only until total >= 0 for each item
        // https://www.amazon.com/gp/help/customer/display.html?nodeId=G9R2MLD3EX557D77
        Map<Integer, Float> totalPerItem = new HashMap<>();
        float total_incentive = 0;
        for(var item : stockConfirmed.items)
        {
            float total_item = item.UnitPrice * item.Quantity;

            if (total_item - item.Voucher > 0)
            {
                total_amount -= item.Voucher;
                total_incentive += item.Voucher;
                total_item -= item.Voucher;
            }
            else
            {
                total_amount -= total_item;
                total_incentive += total_item;
                total_item = 0;
            }

            totalPerItem.put(item.ProductId, total_item);
        }

        CustomerOrder customerOrder = this.customerOrderRepository.lookupByKey( stockConfirmed.customerCheckout.CustomerId );
        if (customerOrder == null)
        {
            customerOrder = new CustomerOrder(stockConfirmed.customerCheckout.CustomerId, 1);
            this.customerOrderRepository.insert(customerOrder);
        } else {
            customerOrder.next_order_id++;
            this.customerOrderRepository.update(customerOrder);
        }

        String invoiceNumber = buildInvoiceNumber( stockConfirmed.customerCheckout.CustomerId, now, customerOrder.next_order_id );

        Order order = new Order(
                 customerOrder.next_order_id,
                 invoiceNumber,
                 customerOrder.customer_id,
                 OrderStatus.INVOICED,
                 stockConfirmed.timestamp,
                null,
                null,
                null,
                null,
                 stockConfirmed.items.size(),
                 now,
                 now,
                 total_amount,
                 total_freight,
                 total_incentive,
                 total_amount + total_freight,
                 total_items
                 );
        this.orderRepository.insert(order);

        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.DATE, 3);

        List<OrderItem> orderItems = new ArrayList<>();
        int item_id = 1;
        for(var item : stockConfirmed.items)
        {
            OrderItem oim = new OrderItem
            (
                order.id,
                item_id,
                item.ProductId,
                item.ProductName,
                item.SellerId,
                item.UnitPrice,
                cal.getTime(),
                item.FreightValue,
                item.Quantity,
                totalPerItem.get(item.ProductId),
                item.UnitPrice * item.Quantity
            );

            orderItems.add(oim);
            this.orderItemRepository.insert(oim);

            item_id++;
        }

        OrderHistory oh = new OrderHistory(
            customerOrder.next_order_id,
            order.created_at,
            OrderStatus.INVOICED);
        this.orderHistoryRepository.insert(oh);

        return new InvoiceIssued( stockConfirmed.customerCheckout, customerOrder.next_order_id, invoiceNumber, now, order.total_invoice,
               orderItems.stream().map(OrderItem::toCommonOrderItem).collect(Collectors.toList()), stockConfirmed.instanceId);
    }

    public static String buildInvoiceNumber(int customerId, Date timestamp, int orderId)
    {
        return new StringBuilder()
                .append(customerId).append('-')
                .append(timestamp.toString()).append('-')
                .append(orderId).toString();
    }

}
