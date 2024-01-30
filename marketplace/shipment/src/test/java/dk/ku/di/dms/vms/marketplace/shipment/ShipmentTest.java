package dk.ku.di.dms.vms.marketplace.shipment;

import dk.ku.di.dms.vms.marketplace.common.entities.CustomerCheckout;
import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.events.PaymentConfirmed;
import dk.ku.di.dms.vms.marketplace.shipment.dtos.OldestSellerPackageEntry;
import dk.ku.di.dms.vms.marketplace.shipment.repositories.IPackageRepository;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static dk.ku.di.dms.vms.marketplace.shipment.ShipmentService.OLDEST_SHIPMENT_PER_SELLER;
import static java.lang.Thread.sleep;

public class ShipmentTest {

    @Test
    public void proxyTest() throws Exception {
        VmsApplication vms = VmsApplication.build("localhost", 8084, new String[]{
                "dk.ku.di.dms.vms.marketplace.shipment",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();

        IPackageRepository packageRepository = (IPackageRepository) vms.getRepositoryProxy("packages");

        Object[] obj = new Object[] { 1, 1, 1, 1, "test", 1.0f, new Date(), new Date(), 1, "shipped"  };

        vms.getTable("packages").underlyingPrimaryKeyIndex().insert(
                KeyUtils.buildKey( new int[]{ 1, 1, 1} ),
                obj
        );

        vms.getTable("packages").secondaryIndexMap.
                get( (IIndexKey) KeyUtils.buildKey( new int[]{ 0, 1} ) )
                .insert(
                    KeyUtils.buildKey( new int[]{ 1, 1 } ),
                    obj
        );

        // packageRepository.getPackagesByCustomerIdAndSellerId(1,1);

        List<OldestSellerPackageEntry> packages = packageRepository.fetchMany(
                OLDEST_SHIPMENT_PER_SELLER, OldestSellerPackageEntry.class);


        assert !packages.isEmpty();
    }

    @Test
    public void test() throws Exception {

        VmsApplication vms = VmsApplication.build("localhost", 8084, new String[]{
                "dk.ku.di.dms.vms.marketplace.shipment",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();

        CustomerCheckout customerCheckout = new CustomerCheckout(
                1, "test", "test", "test", "test","test", "test", "test",
                "CREDIT_CARD","test","test","test", "test", "test", 1,"1");

        PaymentConfirmed paymentConfirmed = new PaymentConfirmed(customerCheckout, 1, 100f,
                List.of(new OrderItem(1,1,1, "name", 1, 1.0f, new Date(), 1.0f, 1, 1.0f, 1.0f, 0.0f) ),
                new Date(), "1");

        InboundEvent inboundEvent = new InboundEvent( 1, 0, 1,
                "payment_confirmed", PaymentConfirmed.class, paymentConfirmed );
        vms.internalChannels().transactionInputQueue().add( inboundEvent );

        sleep(100000);

    }

}
