package dk.ku.di.dms.vms.marketplace.common.events;

import java.util.Date;

public class ShipmentNotification {

    public int customerId;

    public int orderId;
    
    public String shipmentStatus;
    
    public Date eventDate;

    public ShipmentNotification() {
    }

    public ShipmentNotification(int customerId, int orderId, String shipmentStatus, Date eventDate) {
        this.customerId = customerId;
        this.orderId = orderId;
        this.shipmentStatus = shipmentStatus;
        this.eventDate = eventDate;
    }

}
