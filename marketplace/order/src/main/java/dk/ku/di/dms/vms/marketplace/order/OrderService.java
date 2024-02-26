package dk.ku.di.dms.vms.marketplace.order;

import dk.ku.di.dms.vms.marketplace.common.enums.OrderStatus;
import dk.ku.di.dms.vms.marketplace.common.enums.ShipmentStatus;
import dk.ku.di.dms.vms.marketplace.common.events.*;
import dk.ku.di.dms.vms.marketplace.order.entities.CustomerOrder;
import dk.ku.di.dms.vms.marketplace.order.entities.Order;
import dk.ku.di.dms.vms.marketplace.order.entities.OrderHistory;
import dk.ku.di.dms.vms.marketplace.order.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.order.repositories.ICustomerOrderRepository;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderHistoryRepository;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderItemRepository;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("order")
public final class OrderService {

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

    // @AllowOutOfOrderProcessing
    public void processPaymentConfirmed(PaymentConfirmed paymentConfirmed){

    }

    @Inbound(values = {"shipment_updated"})
    @Transactional(type=RW)
    public void processShipmentNotification(ShipmentUpdated shipmentUpdated){
        System.out.println("Order received a shipment updated event with TID: "+ shipmentUpdated.instanceId);

        Date now = new Date();
        for(ShipmentNotification shipmentNotification : shipmentUpdated.shipmentNotifications) {

            Order order = this.orderRepository.lookupByKey( new Order.OrderId(
                    shipmentNotification.customerId, shipmentNotification.orderId
            ) );

            OrderStatus status = OrderStatus.READY_FOR_SHIPMENT;
            if(shipmentNotification.status == ShipmentStatus.DELIVERY_IN_PROGRESS) {
                status = OrderStatus.IN_TRANSIT;
                order.delivered_carrier_date = shipmentNotification.eventDate;
            } else if(shipmentNotification.status == ShipmentStatus.CONCLUDED) {
                status = OrderStatus.DELIVERED;
                order.delivered_customer_date = shipmentNotification.eventDate;
            }

            OrderHistory orderHistory = new OrderHistory(
                    order.customer_id,
                    order.order_id,
                    now,
                    status);

            order.updated_at = now;
            order.status = status;

            this.orderRepository.update(order);
            this.orderHistoryRepository.insert( orderHistory );

        }

    }

//    @PartitionBy(clazz = StockConfirmed.class, method = "getCustomerId")
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
                 customerOrder.customer_id,
                 customerOrder.next_order_id,
                 invoiceNumber,
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
        float total_amount_item;
        for(var item : stockConfirmed.items)
        {
            total_amount_item = item.UnitPrice * item.Quantity;
            OrderItem oim = new OrderItem
            (
                customerOrder.customer_id,
                order.order_id,
                item_id,
                item.ProductId,
                item.ProductName,
                item.SellerId,
                item.UnitPrice,
                cal.getTime(),
                item.FreightValue,
                item.Quantity,
                totalPerItem.get(item.ProductId),
                total_amount_item,
                    total_amount_item - totalPerItem.get(item.ProductId)
            );

            orderItems.add(oim);
            this.orderItemRepository.insert(oim);

            item_id++;
        }

        OrderHistory oh = new OrderHistory(
                customerOrder.customer_id,
                customerOrder.next_order_id,
                order.created_at,
                OrderStatus.INVOICED);
        this.orderHistoryRepository.insert(oh);

        return new InvoiceIssued( stockConfirmed.customerCheckout, customerOrder.next_order_id, invoiceNumber, now, order.total_invoice,
               orderItems.stream().map(OrderItem::toCommonOrderItem).toList(), stockConfirmed.instanceId);
    }

    private static final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");

    public static String buildInvoiceNumber(int customerId, Date timestamp, int orderId)
    {
        return new StringBuilder()
                .append(customerId).append('-')
                .append(df.format(timestamp)).append('-')
                .append(orderId).toString();
    }

}
