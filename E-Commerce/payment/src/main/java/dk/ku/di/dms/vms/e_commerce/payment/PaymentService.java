package dk.ku.di.dms.vms.e_commerce.payment;

import dk.ku.di.dms.vms.e_commerce.common.events.PaymentRequest;
import dk.ku.di.dms.vms.e_commerce.common.events.PaymentResponse;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("payment")
public class PaymentService {

    private final IPaymentRepository paymentRepository;

    private final IAuthorizationRepository authorizationRepository;

    public PaymentService(IPaymentRepository paymentRepository, IAuthorizationRepository authorizationRepository) {
        this.paymentRepository = paymentRepository;
        this.authorizationRepository = authorizationRepository;
    }

    @Inbound(values = "payment-request")
    @Outbound("payment-response")
    @Transactional(type = RW)
    public PaymentResponse processNewPayment(PaymentRequest paymentRequest){

        float amount = authorizationRepository.lookupByKey(1L).declineOverAmount;

        Payment payment = new Payment(paymentRequest.customer, paymentRequest.card, paymentRequest.address);

        if(paymentRequest.amount >= amount){
            payment.authorized = false;
            paymentRepository.insert(payment);
            return new PaymentResponse(false, paymentRequest.amount, "");
        }

        payment.authorized = true;
        return new PaymentResponse(true, paymentRequest.amount, "");

    }

}
