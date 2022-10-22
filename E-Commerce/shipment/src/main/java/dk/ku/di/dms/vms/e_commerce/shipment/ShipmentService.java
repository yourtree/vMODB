package dk.ku.di.dms.vms.e_commerce.shipment;

import dk.ku.di.dms.vms.e_commerce.common.events.ShipmentRequest;
import dk.ku.di.dms.vms.e_commerce.common.events.ShipmentResponse;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.time.LocalDate;
import java.time.LocalTime;

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
        LocalDate shippingDate = null;
        if (LocalTime.now().isAfter(LocalTime.parse("10:00"))
                && LocalTime.now().isBefore(LocalTime.parse("18:00"))) {
            shippingDate = LocalDate.now().plusDays(1);
        } else {
            shippingDate = LocalDate.now().plusDays(2);
        }

        // TODO finish shipment. one shipment per warehouse, all same order
        Shipment shipment = new Shipment(shipmentRequest.orderId, shipmentRequest.customer);

        shipmentRepository.insert(shipment);

        // TODO should we have a table deliveries? one per day. a shipment

        return null;

    }

}
