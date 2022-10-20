package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.e_commerce.common.entity.Address;
import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class NewOrderResult {

    public long order_id;

    public Customer customer;

    public Address address;

}
