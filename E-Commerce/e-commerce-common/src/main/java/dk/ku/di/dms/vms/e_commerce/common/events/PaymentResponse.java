package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.Date;

@Event
public class PaymentResponse {

    public boolean authorized;

    public float amount;
    public String message;

    public Date date;

    public long orderId;

    public PaymentResponse() { }

    public PaymentResponse(boolean authorized, float amount, String message, Date date, long orderId) {
        this.authorized = authorized;
        this.amount = amount;
        this.message = message;
        this.date = date;
    }

}
