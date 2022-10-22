package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class NewOrderRequest {

    public long customerId;
    public long cardId;
    public long addressId;

}
