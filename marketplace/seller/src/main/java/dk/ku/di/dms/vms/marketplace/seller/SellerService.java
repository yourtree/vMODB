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

    @Inbound(values = "invoice_issued")
    @Transactional(type=W)
    @Parallel
    public void processInvoiceIssued(InvoiceIssued invoiceIssued){

        Map<Integer,ReadWriteLock> locksAcquired = new HashMap<>();

        System.out.println("Seller received an invoice issued event with TID: "+ invoiceIssued.instanceId);
        List<OrderItem> orderItems = invoiceIssued.getItems();
        List<OrderEntry> list = new ArrayList<>(invoiceIssued.getItems().size());

        for (OrderItem orderItem : orderItems) {
            var totalInvoice = orderItem.total_amount + orderItem.getFreightValue();
            OrderEntry orderEntry = new OrderEntry(
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
            list.add(orderEntry);

            if(!locksAcquired.containsKey(orderItem.seller_id)) {
                ReadWriteLock sellerLock = this.sellerLockMap.computeIfAbsent(orderItem.seller_id, (x) -> new ReentrantReadWriteLock());
                sellerLock.readLock().lock();
                locksAcquired.put(orderEntry.seller_id, sellerLock);
            }

            // view maintenance code
            orderSellerViewMap.putIfAbsent(orderItem.seller_id, new OrderSellerView(orderItem.seller_id) );
            orderSellerViewMap.compute(orderEntry.seller_id, (sellerId, view) -> {
                view.orders.add(new OrderSellerView.OrderId(orderEntry.customer_id, orderItem.order_id));
                view.count_items++;
                view.total_amount += orderEntry.total_amount;
                view.total_incentive += orderItem.total_incentive;
                view.total_freight += orderItem.freight_value;
                view.total_items += orderItem.total_items;
                view.total_invoice += totalInvoice;
                // this requires maintaining another map
                view.count_orders = view.orders.size();
                return null;
            });

        }

        // unlock all
        for(var lock : locksAcquired.entrySet()){
            lock.getValue().readLock().unlock();
        }

        this.orderEntryRepository.insertAll(list);
    }

    @Inbound(values = "shipment_updated")
    @Transactional(type=RW)
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

    /**
     * It would be better to set a "fake" tid for this transaction, so it could read from the multi version entries
     * However, the view is not being maintained via the MODB =(
     * @param sellerId the seller identifier
     * @return seller dashboard
     */
    public OrderSellerView queryDashboard(int sellerId){
        ReadWriteLock sellerLock = this.sellerLockMap.computeIfAbsent(sellerId, (x) -> new ReentrantReadWriteLock());
        sellerLock.writeLock().lock();
        var res = this.orderSellerViewMap.getOrDefault( sellerId, new OrderSellerView(sellerId) );
        sellerLock.writeLock().unlock();
        return  res;
    }

}
