package dk.ku.di.dms.vms.marketplace.shipment;

import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.enums.ShipmentStatus;
import dk.ku.di.dms.vms.marketplace.common.events.DeliveryNotification;
import dk.ku.di.dms.vms.marketplace.common.events.PaymentConfirmed;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentNotification;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentUpdated;
import dk.ku.di.dms.vms.marketplace.shipment.dtos.OldestSellerPackageEntry;
import dk.ku.di.dms.vms.marketplace.shipment.entities.Package;
import dk.ku.di.dms.vms.marketplace.shipment.entities.PackageStatus;
import dk.ku.di.dms.vms.marketplace.shipment.entities.Shipment;
import dk.ku.di.dms.vms.marketplace.shipment.repositories.IPackageRepository;
import dk.ku.di.dms.vms.marketplace.shipment.repositories.IShipmentRepository;
import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("shipment")
public class ShipmentService {

    private final IShipmentRepository shipmentRepository;

    private final IPackageRepository packageRepository;

    @VmsPreparedStatement("oldestShipmentPerSeller")
    public static final SelectStatement OLDEST_SHIPMENT_PER_SELLER = QueryBuilderFactory.select()
            .project("seller_id").project("customer_id").project("order_id").min("shipping_date")
            .from("packages").where("status", ExpressionTypeEnum.EQUALS, "shipped")
            .groupBy( "seller_id" ).limit(10).build();

    public ShipmentService(IShipmentRepository shipmentRepository, IPackageRepository packageRepository){
        this.shipmentRepository = shipmentRepository;
        this.packageRepository = packageRepository;
    }

    @Inbound(values = {"update_shipment"})
    @Outbound("shipment_updated")
    @Transactional(type=RW)
    public ShipmentUpdated updateShipment(String instanceId){

        System.out.println("Shipment received an update shipment event with TID: "+ instanceId);
        Date now = new Date();

        List<OldestSellerPackageEntry> packages = this.packageRepository.fetchMany(OLDEST_SHIPMENT_PER_SELLER, OldestSellerPackageEntry.class);

//        List<Package> packages = this.packageRepository.getAll(aPackage -> aPackage.status == PackageStatus.shipped);
//        Map<Integer, Package.PackageId> oldestOpenShipmentPerSeller = packages.stream()
////                .filter( p -> p.status.equals(PackageStatus.shipped) )
//                .collect(Collectors.groupingBy(Package::getSellerId,
//                        Collectors.minBy(Comparator.comparing( Package::getShippingDate ))
//                          ) ).entrySet().stream()
//                .filter(entry -> entry.getValue().isPresent())
//                .limit(10) // Limit the result to the first 10 sellers
//                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get().getId()));

        List<ShipmentNotification> shipmentNotifications = new ArrayList<>();
        List<DeliveryNotification> deliveryNotifications = new ArrayList<>();

        for (var entry : packages) {

//            SelectStatement selectStatement = QueryBuilderFactory.select().project("*")
//                    .from("packages")
//                    //.where( "seller_id", ExpressionTypeEnum.EQUALS, entry[0] )
//                    .where( "customer_id", ExpressionTypeEnum.EQUALS, entry[1] )
//                    .and( "order_id", ExpressionTypeEnum.EQUALS, entry[2] )
//                    .build();

            List<Package> orderPackages = packageRepository.getPackagesByCustomerIdAndSellerId( entry.getCustomerId(), entry.getOrderId() );

//            List<Package> sellerPackages = packages.stream().filter( p-> p.seller_id == kv.getKey() && p.order_id == kv.getValue().order_id && p.customer_id == kv.getValue().customer_id ).toList();

            Shipment shipment = this.shipmentRepository.lookupByKey( new Shipment.ShipmentId( entry.getCustomerId(), entry.getOrderId() ));

            if(shipment.status == ShipmentStatus.APPROVED){
                shipment.status = ShipmentStatus.DELIVERY_IN_PROGRESS;
                this.shipmentRepository.update( shipment );
                shipmentNotifications.add( new ShipmentNotification( shipment.customer_id, shipment.order_id, ShipmentStatus.DELIVERY_IN_PROGRESS, now ) );
            }

            long countDelivered = orderPackages.stream().filter(p-> p.status == PackageStatus.delivered).count();
            var sellerPackages = orderPackages.stream().filter(p->p.seller_id == entry.getSellerId()).toList();

            for (Package pkg : sellerPackages) {

                pkg.status = PackageStatus.delivered;
                pkg.delivery_date = now;

                DeliveryNotification deliveryNotification = new DeliveryNotification(
                        shipment.customer_id,
                        pkg.order_id,
                        pkg.package_id,
                        pkg.seller_id,
                        pkg.product_id,
                        pkg.product_name,
                        PackageStatus.delivered.name(),
                        now
                );
                deliveryNotifications.add(deliveryNotification);
            }

            if (shipment.package_count == countDelivered + sellerPackages.size()) {
                shipment.status = ShipmentStatus.CONCLUDED;
                this.shipmentRepository.update( shipment );
                shipmentNotifications.add( new ShipmentNotification( shipment.customer_id, shipment.order_id, ShipmentStatus.CONCLUDED, now ) );
            }

        }

        return new ShipmentUpdated(deliveryNotifications, shipmentNotifications, instanceId );

    }

    @Inbound(values = {"payment_confirmed"})
    @Transactional(type=W)
    public void processShipment(PaymentConfirmed paymentConfirmed){

        System.out.println("Shipment received a payment confirmed event with TID: "+ paymentConfirmed.instanceId);
        Date now = new Date();

        Shipment shipment = new Shipment(
                paymentConfirmed.customerCheckout.CustomerId,
                paymentConfirmed.orderId,
                paymentConfirmed.items.size(),
                (float) paymentConfirmed.items.stream().mapToDouble(OrderItem::getFreightValue).sum(),
                now,
                ShipmentStatus.APPROVED,
                paymentConfirmed.customerCheckout.FirstName,
                paymentConfirmed.customerCheckout.LastName,
                paymentConfirmed.customerCheckout.Street,
                paymentConfirmed.customerCheckout.Complement,
                paymentConfirmed.customerCheckout.ZipCode,
                paymentConfirmed.customerCheckout.City,
                paymentConfirmed.customerCheckout.State
        );

        this.shipmentRepository.insert( shipment );

        int packageId = 1;
        List<dk.ku.di.dms.vms.marketplace.shipment.entities.Package> packages = new ArrayList<>();
        for (OrderItem orderItem : paymentConfirmed.items) {
            dk.ku.di.dms.vms.marketplace.shipment.entities.Package pkg
                    = new dk.ku.di.dms.vms.marketplace.shipment.entities.Package(
                    paymentConfirmed.customerCheckout.CustomerId,
                    paymentConfirmed.orderId,
                    packageId,
                    orderItem.seller_id,
                    orderItem.product_id,
                    orderItem.product_name,
                    orderItem.getFreightValue(),
                    now,
                    null,
                    orderItem.quantity,
                    PackageStatus.shipped
            );
            packages.add(pkg);
            packageId++;
        }

        this.packageRepository.insertAll( packages );

    }

}
