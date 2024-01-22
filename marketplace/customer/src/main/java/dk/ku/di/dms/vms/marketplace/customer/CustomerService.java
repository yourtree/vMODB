package dk.ku.di.dms.vms.marketplace.customer;

import dk.ku.di.dms.vms.marketplace.common.events.DeliveryNotification;
import dk.ku.di.dms.vms.marketplace.common.events.PaymentConfirmed;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentUpdated;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.Date;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("customer")
public class CustomerService {

    private final ICustomerRepository customerRepository;

    public CustomerService(ICustomerRepository customerRepository){
        this.customerRepository = customerRepository;
    }

    @Inbound(values = {"payment_confirmed"})
    @Transactional(type=W)
    public void processPaymentConfirmed(PaymentConfirmed paymentConfirmed){
        Date now = new Date();
        Customer customer = this.customerRepository.lookupByKey( paymentConfirmed.customerCheckout.CustomerId );
        customer.success_payment_count++;
        customer.updated_at = now;
        this.customerRepository.update(customer);
    }

    @Inbound(values = {"shipment_updated"})
    @Transactional(type=W)
    public void processDeliveryNotification(ShipmentUpdated shipmentUpdated){

        Date now = new Date();
        for(DeliveryNotification delivery : shipmentUpdated.deliveryNotifications) {
            Customer customer = this.customerRepository.lookupByKey( delivery.customerId );
            customer.delivery_count++;
            customer.updated_at = now;
            this.customerRepository.update(customer);
        }

    }

}
