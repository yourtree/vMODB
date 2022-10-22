package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.e_commerce.common.entity.Item;
import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.Date;
import java.util.List;

@Event
public class ShipmentRequest {

    public List<Item> items;

    public Customer customer;

    public Date paymentDate;

    public long orderId;

    public ShipmentRequest(List<Item> items, Customer customer, Date paymentDate, long orderId) {
        this.items = items;
        this.customer = customer;
        this.paymentDate = paymentDate;
        this.orderId = orderId;
    }
}
