package dk.ku.di.dms.vms.marketplace.order;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.CustomerCheckout;
import dk.ku.di.dms.vms.marketplace.common.events.StockConfirmed;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static java.lang.Thread.sleep;

public class OrderTest {

    @Test
    public void test() throws Exception {

        VmsApplicationOptions options = new VmsApplicationOptions("localhost", Constants.ORDER_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.order",
                "dk.ku.di.dms.vms.marketplace.common"
        }, 4096, 2, 1000);

        VmsApplication vms = VmsApplication.build(options);
        vms.start();

        CustomerCheckout customerCheckout = new CustomerCheckout(
                1, "test", "test", "test", "test","test", "test", "test",
                "CREDIT_CARD","test","test","test", "test", "test", 1,"1");

        StockConfirmed stockConfirmed = new StockConfirmed(
                new Date(),
                customerCheckout,
                List.of( new CartItem(1,1,"test",1.0f, 1.0f, 1, 1.0f, "1") ),
                "1" );

        InboundEvent inboundEvent = new InboundEvent( 1, 0, 1,
                "stock_confirmed", StockConfirmed.class, stockConfirmed );
        vms.internalChannels().transactionInputQueue().add( inboundEvent );

        sleep(100000);

    }

}
