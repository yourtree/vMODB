package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class PaymentRequest {

    public float amount;

    public Customer customer;

    public long orderId;

    public PaymentRequest(){}

    public PaymentRequest(long orderId, float amount, Customer customer) {
        this.orderId = orderId;
        this.amount = amount;
        this.customer = customer;
    }
}
