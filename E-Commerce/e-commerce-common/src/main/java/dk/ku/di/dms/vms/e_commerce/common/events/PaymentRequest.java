package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.e_commerce.common.entity.Address;
import dk.ku.di.dms.vms.e_commerce.common.entity.Card;
import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class PaymentRequest {

    public float amount;

    public Customer customer;

    public Address address;

    public Card card;

    public PaymentRequest(){}

    public PaymentRequest(float amount, Customer customer, Address address, Card card) {
        this.amount = amount;
        this.customer = customer;
        this.address = address;
        this.card = card;
    }
}
