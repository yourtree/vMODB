package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class NewOrderUserResponse {

    public Customer customer;

    public NewOrderUserResponse(Customer customer) {
        this.customer = customer;
    }
}
