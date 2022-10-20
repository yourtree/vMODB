package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class PaymentResponse {

    public boolean authorised;

    public float debitAmount;
    public String message;

    public PaymentResponse() {
    }

    public PaymentResponse(boolean authorised, float debitAmount, String message) {
        this.authorised = authorised;
        this.debitAmount = debitAmount;
        this.message = message;
    }

}
