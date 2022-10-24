package dk.ku.di.dms.vms.e_commerce.shipment;

import dk.ku.di.dms.vms.e_commerce.common.events.ShipmentRequest;
import dk.ku.di.dms.vms.e_commerce.common.events.ShipmentResponse;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("shipment")
public class ShipmentService {

    private final IShipmentRepository shipmentRepository;

    public ShipmentService(IShipmentRepository shipmentRepository) {
        this.shipmentRepository = shipmentRepository;
    }

    @Inbound(values = "shipment-request")
    @Outbound("shipment-response")
    @Transactional(type = RW)
    public ShipmentResponse processNewShipmentRequest(ShipmentRequest shipmentRequest){

        // inspiration from https://www.baeldung.com/java-reactive-systems
        LocalTime shippingDate = null;
        LocalTime localDate = LocalTime.now();
        var noon = LocalTime.of(12,0);
        if(localDate.isBefore(noon)
                && localDate.isAfter(LocalTime.of(5,59))) {
            shippingDate = noon; // today
        } else {
            shippingDate = noon.plus(1, ChronoUnit.DAYS);

        }

        // TODO one shipment per warehouse, all same order?
        Shipment shipment = new Shipment(
                shipmentRequest.orderId,
                shipmentRequest.customer,
                shippingDate);

        shipmentRepository.insert(shipment);

        // TODO should we have a table deliveries? one per day. a shipment

        return new ShipmentResponse(shippingDate, shipment.orderId);

    }

}
