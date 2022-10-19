package dk.ku.di.dms.vms.e_commerce.payment;

import dk.ku.di.dms.vms.e_commerce.common.events.PaymentRequest;
import dk.ku.di.dms.vms.e_commerce.common.events.PaymentResponse;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;

@Microservice("payment")
public class PaymentService {

    private final IPaymentRepository paymentRepository;


    public PaymentService(IPaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    public PaymentResponse processNewPayment(PaymentRequest paymentRequest){
        return null;
    }

}
