package dk.ku.di.dms.vms.marketplace.payment;

import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.entities.CustomerCheckout;
import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.events.InvoiceIssued;
import dk.ku.di.dms.vms.marketplace.payment.entities.OrderPayment;
import dk.ku.di.dms.vms.marketplace.payment.entities.OrderPaymentCard;
import dk.ku.di.dms.vms.marketplace.payment.enums.PaymentStatus;
import dk.ku.di.dms.vms.marketplace.payment.enums.PaymentType;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static java.lang.Thread.sleep;

public class PaymentTest {

    @Test
    public void test() throws Exception {

        VmsApplication vms = VmsApplication.build("localhost", 8084, new String[]{
                "dk.ku.di.dms.vms.marketplace.payment",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();

        CustomerCheckout customerCheckout = new CustomerCheckout(
                1, "test", "test", "test", "test","test", "test", "test",
                "CREDIT_CARD","test","test","test", "test", "test", 1,"1");

        InvoiceIssued invoiceIssued = new InvoiceIssued( customerCheckout, 1,  "test", new Date(), 100,
                List.of(new OrderItem(1,1,1, "name", 1, 1.0f, new Date(), 1.0f, 1, 1.0f, 1.0f, 0.0f) )
                , "1");

        InboundEvent inboundEvent = new InboundEvent( 1, 0, 1,
                "invoice_issued", InvoiceIssued.class, invoiceIssued );
        vms.internalChannels().transactionInputQueue().add( inboundEvent );

        sleep(100000);

    }

}
