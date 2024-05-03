package dk.ku.di.dms.vms.marketplace.customer;

import dk.ku.di.dms.vms.marketplace.common.events.DeliveryNotification;
import dk.ku.di.dms.vms.marketplace.common.events.PaymentConfirmed;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentUpdated;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.Date;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.marketplace.common.Constants.PAYMENT_CONFIRMED;
import static dk.ku.di.dms.vms.marketplace.common.Constants.SHIPMENT_UPDATED;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("customer")
public final class CustomerService {

    private static final Logger LOGGER = Logger.getLogger(CustomerService.class.getCanonicalName());

    private final ICustomerRepository customerRepository;

    public CustomerService(ICustomerRepository customerRepository){
        this.customerRepository = customerRepository;
    }

    @Inbound(values = {PAYMENT_CONFIRMED})
    @Transactional(type=RW)
    public void processPaymentConfirmed(PaymentConfirmed paymentConfirmed){
        System.out.println("Customer received a payment confirmed event with TID: "+ paymentConfirmed.instanceId);

        Date now = new Date();
        Customer customer = this.customerRepository.lookupByKey( paymentConfirmed.customerCheckout.CustomerId );

        if(customer == null){
            LOGGER.severe("Customer "+paymentConfirmed.customerCheckout.CustomerId+" cannot be found!");
            return;
        }

        customer.success_payment_count++;
        customer.updated_at = now;
        this.customerRepository.update(customer);
    }

    @Inbound(values = {SHIPMENT_UPDATED})
    @Transactional(type=RW)
    public void processDeliveryNotification(ShipmentUpdated shipmentUpdated){
        System.out.println("Customer received a shipment updated event with TID: "+ shipmentUpdated.instanceId);

        Date now = new Date();
        for(DeliveryNotification delivery : shipmentUpdated.deliveryNotifications) {
            Customer customer = this.customerRepository.lookupByKey( delivery.customerId );
            customer.delivery_count++;
            customer.updated_at = now;
            this.customerRepository.update(customer);
        }
    }

}
