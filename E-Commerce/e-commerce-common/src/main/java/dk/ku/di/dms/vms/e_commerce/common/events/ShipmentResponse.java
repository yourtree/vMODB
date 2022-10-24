package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.time.LocalTime;

@Event
public class ShipmentResponse {

    public ShipmentResponse(LocalTime shippingDate, long orderId) {
        this.shippingDate = shippingDate;
        this.orderId = orderId;
    }

    public LocalTime shippingDate;
    public long orderId;



}
