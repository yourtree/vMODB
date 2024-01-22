package dk.ku.di.dms.vms.marketplace.shipment;

import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.events.DeliveryNotification;
import dk.ku.di.dms.vms.marketplace.common.events.PaymentConfirmed;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentNotification;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentUpdated;
import dk.ku.di.dms.vms.marketplace.shipment.entities.Package;
import dk.ku.di.dms.vms.marketplace.shipment.entities.PackageStatus;
import dk.ku.di.dms.vms.marketplace.shipment.entities.Shipment;
import dk.ku.di.dms.vms.marketplace.shipment.entities.ShipmentStatus;
import dk.ku.di.dms.vms.marketplace.shipment.repositories.IPackageRepository;
import dk.ku.di.dms.vms.marketplace.shipment.repositories.IShipmentRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.*;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("shipment")
public class ShipmentService {

    private final IShipmentRepository shipmentRepository;

    private final IPackageRepository packageRepository;

    public ShipmentService(IShipmentRepository shipmentRepository, IPackageRepository packageRepository){
        this.shipmentRepository = shipmentRepository;
        this.packageRepository = packageRepository;
    }

    @Inbound(values = {"update_shipment"})
    @Transactional(type=RW)
    public ShipmentUpdated updateShipment(String instanceId){

        Date now = new Date();

        List<Package> packages = this.packageRepository.getAll();

        Map<Integer, Package.PackageId> oldestOpenShipmentPerSeller = packages.stream()
                .filter( p -> p.status.equals(PackageStatus.shipped) )
                .collect(Collectors.groupingBy(Package::getSellerId,
                        Collectors.minBy(Comparator.comparing( Package::getShippingDate ))
                          ) ).entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .limit(10) // Limit the result to the first 10 sellers
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get().getId()));


        List<ShipmentNotification> shipmentNotifications = new ArrayList<>();
        List<DeliveryNotification> deliveryNotifications = new ArrayList<>();

        for (Map.Entry<Integer, Package.PackageId> kv : oldestOpenShipmentPerSeller.entrySet()) {
            List<Package> sellerPackages = packages.stream().filter( p-> p.seller_id == kv.getKey() && p.order_id == kv.getValue().order_id && p.customer_id == kv.getValue().customer_id ).toList();

            Shipment shipment = this.shipmentRepository.lookupByKey( new Shipment.ShipmentId( kv.getValue().customer_id, kv.getValue().order_id ) );

            if(shipment.status == ShipmentStatus.APPROVED){
                shipment.status = ShipmentStatus.DELIVERY_IN_PROGRESS;
                this.shipmentRepository.update( shipment );
                shipmentNotifications.add( new ShipmentNotification( shipment.customer_id, shipment.order_id, ShipmentStatus.DELIVERY_IN_PROGRESS.name(), now ) );
            }

            long countDelivered = packages.stream().filter(p-> p.customer_id == kv.getValue().customer_id && p.order_id == kv.getValue().order_id && p.status == PackageStatus.delivered).count();

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
                shipmentNotifications.add( new ShipmentNotification( shipment.customer_id, shipment.order_id, ShipmentStatus.CONCLUDED.name(), now ) );
            }

        }

        return new ShipmentUpdated(deliveryNotifications, shipmentNotifications, instanceId );

    }

    @Inbound(values = {"payment_confirmed"})
    @Transactional(type=W)
    public void processShipment(PaymentConfirmed paymentConfirmed){

        Date now = new Date();

        Shipment shipment = new Shipment(
                paymentConfirmed.customerCheckout.CustomerId,
                paymentConfirmed.orderId,
                paymentConfirmed.items.size(),
                (float)  paymentConfirmed.items.stream().mapToDouble(OrderItem::getFreightValue).sum(),
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
