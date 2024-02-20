package dk.ku.di.dms.vms.marketplace.seller;

import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.enums.OrderStatus;
import dk.ku.di.dms.vms.marketplace.common.enums.PackageStatus;
import dk.ku.di.dms.vms.marketplace.common.enums.ShipmentStatus;
import dk.ku.di.dms.vms.marketplace.common.events.DeliveryNotification;
import dk.ku.di.dms.vms.marketplace.common.events.InvoiceIssued;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentNotification;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentUpdated;
import dk.ku.di.dms.vms.marketplace.seller.entities.OrderEntry;
import dk.ku.di.dms.vms.marketplace.seller.repositories.IOrderEntryRepository;
import dk.ku.di.dms.vms.marketplace.seller.repositories.ISellerRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Parallel;

import java.util.ArrayList;
import java.util.List;

@Microservice("seller")
public final class SellerService {

    private final ISellerRepository sellerRepository;

    private final IOrderEntryRepository orderEntryRepository;

    public SellerService(ISellerRepository sellerRepository, IOrderEntryRepository orderEntryRepository){
        this.sellerRepository = sellerRepository;
        this.orderEntryRepository = orderEntryRepository;
    }

    @Inbound(values = "invoice_issued")
    @Parallel
    public void processInvoiceIssued(InvoiceIssued invoiceIssued){
        System.out.println("Seller received an invoice issued event with TID: "+ invoiceIssued.instanceId);
        List<OrderItem> orderItems = invoiceIssued.getItems();
        List<OrderEntry> list = new ArrayList<>(invoiceIssued.getItems().size());

        for (OrderItem orderItem : orderItems) {
            OrderEntry orderEntry = new OrderEntry(
                    invoiceIssued.customer.CustomerId,
                    invoiceIssued.orderId,
                    -1,
                    orderItem.seller_id,
                    orderItem.product_id,
                    orderItem.product_name,
                    "",
                    orderItem.unit_price,
                    orderItem.quantity,
                    orderItem.total_items,
                    orderItem.total_amount,
                    orderItem.total_amount + orderItem.getFreightValue(),
                    orderItem.total_incentive,
                    orderItem.freight_value,
                    null,
                    null,
                    OrderStatus.INVOICED,
                    null
            );
            list.add(orderEntry);
        }
        this.orderEntryRepository.insertAll(list);
    }

    @Inbound(values = "shipment_updated")
    public void processShipmentUpdate(ShipmentUpdated shipmentUpdated){
        System.out.println("Seller received a shipment update event with TID: "+ shipmentUpdated.instanceId);
        for(ShipmentNotification shipmentNotification : shipmentUpdated.shipmentNotifications) {
            List<OrderEntry> orderEntries = this.orderEntryRepository.getOrderEntriesByCustomerIdAndOrderId(
                    shipmentNotification.customerId, shipmentNotification.orderId );
            for(OrderEntry entry : orderEntries){
                if (shipmentNotification.status == ShipmentStatus.APPROVED) {
                    entry.order_status = OrderStatus.READY_FOR_SHIPMENT;
                    entry.shipment_date = shipmentNotification.eventDate;
                    entry.delivery_status = PackageStatus.ready_to_ship;
                } else if (shipmentNotification.status == ShipmentStatus.DELIVERY_IN_PROGRESS) {
                    entry.order_status = OrderStatus.IN_TRANSIT;
                    entry.delivery_status = PackageStatus.shipped;
                } else if (shipmentNotification.status == ShipmentStatus.CONCLUDED) {
                    entry.order_status = OrderStatus.DELIVERED;
                }
                this.orderEntryRepository.update( entry );
            }
        }

        for(DeliveryNotification delivery : shipmentUpdated.deliveryNotifications) {
            OrderEntry orderEntry = this.orderEntryRepository.lookupByKey(
                    new OrderEntry.OrderEntryId( delivery.customerId, delivery.orderId, delivery.productId ) );
            orderEntry.delivery_status = delivery.packageStatus;
            orderEntry.delivery_date = delivery.deliveryDate;
            orderEntry.package_id = delivery.packageId;
            this.orderEntryRepository.update( orderEntry );
        }

    }

}
