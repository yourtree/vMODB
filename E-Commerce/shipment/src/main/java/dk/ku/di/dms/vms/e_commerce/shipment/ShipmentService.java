package dk.ku.di.dms.vms.e_commerce.shipment;

import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderResult;
import dk.ku.di.dms.vms.e_commerce.common.events.PaymentResponse;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("shipment")
public class ShipmentService {

    private final IShipmentRepository shipmentRepository;

    public ShipmentService(IShipmentRepository shipmentRepository) {
        this.shipmentRepository = shipmentRepository;
    }

    @Inbound(values = "new-order-result")
    @Outbound("payment-response")
    @Transactional(type = RW)
    public PaymentResponse processNewShipment(NewOrderResult newOrderResult){

        // TODO finish https://www.baeldung.com/java-reactive-systems
        return null;

    }

}
