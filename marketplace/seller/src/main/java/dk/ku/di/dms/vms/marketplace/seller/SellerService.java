package dk.ku.di.dms.vms.marketplace.seller;

import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.enums.OrderStatus;
import dk.ku.di.dms.vms.marketplace.common.enums.PackageStatus;
import dk.ku.di.dms.vms.marketplace.common.enums.ShipmentStatus;
import dk.ku.di.dms.vms.marketplace.common.events.DeliveryNotification;
import dk.ku.di.dms.vms.marketplace.common.events.InvoiceIssued;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentNotification;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentUpdated;
import dk.ku.di.dms.vms.marketplace.seller.dtos.OrderSellerView;
import dk.ku.di.dms.vms.marketplace.seller.entities.OrderEntry;
import dk.ku.di.dms.vms.marketplace.seller.repositories.IOrderEntryRepository;
import dk.ku.di.dms.vms.marketplace.seller.repositories.ISellerRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Parallel;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static dk.ku.di.dms.vms.marketplace.common.Constants.INVOICE_ISSUED;
import static dk.ku.di.dms.vms.marketplace.common.Constants.SHIPMENT_UPDATED;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("seller")
public final class SellerService {

    // support isolation for seller dashboard retrieval
    private final Map<Integer, ReadWriteLock> sellerLockMap;

    // necessary to force vms loader to load this repository
    private final ISellerRepository sellerRepository;

    private final IOrderEntryRepository orderEntryRepository;

    private final Map<Integer, OrderSellerView> orderSellerViewMap;

    public SellerService(ISellerRepository sellerRepository, IOrderEntryRepository orderEntryRepository){
        this.sellerRepository = sellerRepository;
        this.orderEntryRepository = orderEntryRepository;
        this.orderSellerViewMap = new ConcurrentHashMap<>(10000);
        this.sellerLockMap = new ConcurrentHashMap<>(10000);
    }

    @Inbound(values = INVOICE_ISSUED)
    @Transactional(type=W)
    @Parallel
    public void processInvoiceIssued(InvoiceIssued invoiceIssued){
        System.out.println("APP: Seller received an invoice issued event with TID: "+ invoiceIssued.instanceId);

        Map<Integer,ReadWriteLock> locksAcquired = new HashMap<>();
        List<OrderItem> orderItems = invoiceIssued.getItems();
        List<OrderEntry> list = new ArrayList<>(invoiceIssued.getItems().size());

        for (OrderItem orderItem : orderItems) {
            float totalInvoice = orderItem.total_amount + orderItem.getFreightValue();
            OrderEntry entry = new OrderEntry(
                    invoiceIssued.customer.CustomerId,
                    invoiceIssued.orderId,
                    orderItem.product_id,
                    orderItem.seller_id,
                    -1, // unknown at this point
                    orderItem.product_name,
                    "",
                    orderItem.unit_price,
                    orderItem.quantity,
                    orderItem.total_items,
                    orderItem.total_amount,
                    totalInvoice,
                    orderItem.total_incentive,
                    orderItem.freight_value,
                    null,
                    null,
                    OrderStatus.INVOICED,
                    null
            );
            list.add(entry);

            if(!locksAcquired.containsKey(orderItem.seller_id)) {
                ReadWriteLock sellerLock = this.sellerLockMap.computeIfAbsent(orderItem.seller_id, (ignored) -> new ReentrantReadWriteLock());
                sellerLock.writeLock().lock();
                locksAcquired.put(entry.seller_id, sellerLock);
            }

            // view maintenance code
            // orderSellerViewMap.putIfAbsent(orderItem.seller_id, new OrderSellerView(orderItem.seller_id) );
            OrderSellerView view;
            if(this.orderSellerViewMap.containsKey(orderItem.seller_id)){
                view = this.orderSellerViewMap.get(orderItem.seller_id);
            } else {
                view = new OrderSellerView(orderItem.seller_id);
                this.orderSellerViewMap.put(orderItem.seller_id, view);
            }

            view.orders.add(new OrderSellerView.OrderId(entry.customer_id, orderItem.order_id));
            view.count_items += entry.quantity;
            view.total_amount += entry.total_amount;
            view.total_incentive += orderItem.total_incentive;
            view.total_freight += orderItem.freight_value;
            view.total_items += orderItem.total_items;
            view.total_invoice += totalInvoice;
            // this requires maintaining another map
            view.count_orders = view.orders.size();
        }

        // unlock all
        for(Map.Entry<Integer, ReadWriteLock> lock : locksAcquired.entrySet()){
            lock.getValue().writeLock().unlock();
        }

        this.orderEntryRepository.insertAll(list);
    }

    @Inbound(values = SHIPMENT_UPDATED)
    @Transactional(type=RW)
    public void processShipmentUpdate(ShipmentUpdated shipmentUpdated){
        System.out.println("APP: Seller received a shipment update event with TID: "+ shipmentUpdated.instanceId);

        // TODO synchronization must also be present here

        for(ShipmentNotification shipmentNotification : shipmentUpdated.shipmentNotifications) {
            List<OrderEntry> orderEntries = this.orderEntryRepository.getOrderEntriesByCustomerIdAndOrderId(
                    shipmentNotification.customerId, shipmentNotification.orderId );
            for(OrderEntry entry : orderEntries){

                if(entry.delivery_status == PackageStatus.delivered) continue;

                switch (shipmentNotification.status) {
                    case ShipmentStatus.APPROVED -> {
                        entry.order_status = OrderStatus.READY_FOR_SHIPMENT;
                        entry.shipment_date = shipmentNotification.eventDate;
                        entry.delivery_status = PackageStatus.ready_to_ship;
                    }
                    case ShipmentStatus.DELIVERY_IN_PROGRESS -> {
                        entry.order_status = OrderStatus.IN_TRANSIT;
                        entry.delivery_status = PackageStatus.shipped;
                    }
                    case ShipmentStatus.CONCLUDED -> {
                        // remove entry from view
                        entry.order_status = OrderStatus.DELIVERED;
                        OrderSellerView view = this.orderSellerViewMap.get(entry.seller_id);
                        view.orders.remove(new OrderSellerView.OrderId(entry.customer_id, entry.order_id));
                        view.count_items -= entry.quantity;
                        view.total_amount -= entry.total_amount;
                        view.total_incentive -= entry.total_incentive;
                        view.total_freight -= entry.freight_value;
                        view.total_items -= entry.total_items;
                        view.total_invoice -= entry.total_invoice;
                        // this requires maintaining another map
                        view.count_orders = view.orders.size();
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + shipmentNotification.status);
                }
                this.orderEntryRepository.update( entry );
            }
        }

        for(DeliveryNotification delivery : shipmentUpdated.deliveryNotifications) {
            OrderEntry orderEntry = this.orderEntryRepository.lookupByKey(
                    new OrderEntry.OrderEntryId( delivery.customerId, delivery.orderId, delivery.productId ) );

            if(orderEntry.delivery_status == PackageStatus.delivered) continue;

            orderEntry.delivery_status = delivery.packageStatus;
            orderEntry.delivery_date = delivery.deliveryDate;
            orderEntry.package_id = delivery.packageId;
            this.orderEntryRepository.update( orderEntry );
        }

    }

    /**
     * It would be better to set a "fake" tid for this transaction, so it could read from the multi version entries
     * However, the view is not being maintained via the MODB =(
     * @param sellerId the seller identifier
     * @return seller dashboard
     */
    public OrderSellerView queryDashboard(int sellerId){
        ReadWriteLock sellerLock = this.sellerLockMap.computeIfAbsent(sellerId, (ignored) -> new ReentrantReadWriteLock());
        sellerLock.readLock().lock();
        OrderSellerView res = this.orderSellerViewMap.getOrDefault( sellerId, new OrderSellerView(sellerId) );
        sellerLock.readLock().unlock();
        return res;
    }

}
