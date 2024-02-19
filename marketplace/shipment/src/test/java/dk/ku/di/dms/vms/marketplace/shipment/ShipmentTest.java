package dk.ku.di.dms.vms.marketplace.shipment;

import dk.ku.di.dms.vms.marketplace.common.entities.CustomerCheckout;
import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.events.PaymentConfirmed;
import dk.ku.di.dms.vms.marketplace.shipment.dtos.OldestSellerPackageEntry;
import dk.ku.di.dms.vms.marketplace.shipment.repositories.IPackageRepository;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.UniqueSecondaryIndex;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;

import static dk.ku.di.dms.vms.marketplace.shipment.ShipmentService.OLDEST_SHIPMENT_PER_SELLER;
import static java.lang.Thread.sleep;

public final class ShipmentTest {

    private static VmsApplication getVmsApplication() throws Exception {
        return VmsApplication.build("localhost", 8084, new String[]{
                "dk.ku.di.dms.vms.marketplace.shipment",
                "dk.ku.di.dms.vms.marketplace.common"
        });
    }

    private static final BiFunction<Integer, String, CustomerCheckout> customerCheckoutBiFunction = (customerId, instanceId) -> new CustomerCheckout(
            customerId, "test", "test", "test", "test","test", "test", "test",
            "CREDIT_CARD","test","test","test", "test", "test", 1,instanceId);

    private static final BiFunction<CustomerCheckout, Integer, PaymentConfirmed> paymentConfirmedBiFunction = (customerCheckout, sellerId) -> new PaymentConfirmed(customerCheckout, 1, 100f,
            List.of(new OrderItem(sellerId,1,1, "name", sellerId, 1.0f, new Date(), 1.0f, 1, 1.0f, 1.0f, 0.0f) ),
            new Date(), customerCheckout.instanceId);

    @Test
    public void testPackageQueryMultiversionVisibility() throws Exception {
        VmsApplication vms = getVmsApplication();
        vms.start();

        for(int i = 1; i < 4; i++) {
            CustomerCheckout customerCheckout = customerCheckoutBiFunction.apply(i, String.valueOf(i));
            PaymentConfirmed paymentConfirmed = paymentConfirmedBiFunction.apply(customerCheckout, i);

            InboundEvent inboundEvent = new InboundEvent(i, i-1, 1,
                    "payment_confirmed", PaymentConfirmed.class, paymentConfirmed);
            vms.internalChannels().transactionInputQueue().add(inboundEvent);
        }

        sleep(2000);

        TransactionMetadata.registerTransactionStart( 4, 0, 3, true );

        IPackageRepository packageRepository = (IPackageRepository) vms.getRepositoryProxy("packages");
        List<dk.ku.di.dms.vms.marketplace.shipment.entities.Package> list =
                packageRepository.getPackagesByCustomerIdAndOrderId(1,1);

        assert list.size() == 1;
    }

    @Test
    public void testOldestPackagePerSeller() throws Exception {
        VmsApplication vms = getVmsApplication();
        vms.start();

        IPackageRepository packageRepository = (IPackageRepository) vms.getRepositoryProxy("packages");

        for(int i = 1; i <= 10; i++) {
            CustomerCheckout customerCheckout = customerCheckoutBiFunction.apply(i, String.valueOf(i));
            PaymentConfirmed paymentConfirmed = paymentConfirmedBiFunction.apply(customerCheckout, i);

            InboundEvent inboundEvent = new InboundEvent(i, i-1, 1,
                    "payment_confirmed", PaymentConfirmed.class, paymentConfirmed);
            vms.internalChannels().transactionInputQueue().add(inboundEvent);
        }

        sleep(5000);

        TransactionMetadata.registerTransactionStart( 11, 0, 10, true );

        List<OldestSellerPackageEntry> packages = packageRepository.query(
                OLDEST_SHIPMENT_PER_SELLER, OldestSellerPackageEntry.class);

        assert packages.size() == 10;
    }

    @Test
    public void test() throws Exception {
        VmsApplication vms = getVmsApplication();
        vms.start();

        CustomerCheckout customerCheckout = customerCheckoutBiFunction.apply(1,"1");
        PaymentConfirmed paymentConfirmed = paymentConfirmedBiFunction.apply(customerCheckout, 1);

        InboundEvent inboundEvent = new InboundEvent( 1, 0, 1,
                "payment_confirmed", PaymentConfirmed.class, paymentConfirmed );
        vms.internalChannels().transactionInputQueue().add( inboundEvent );

        sleep(100000);
    }

}
