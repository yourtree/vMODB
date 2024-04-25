package dk.ku.di.dms.vms.marketplace.shipment;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.events.PaymentConfirmed;
import dk.ku.di.dms.vms.marketplace.shipment.dtos.OldestSellerPackageEntry;
import dk.ku.di.dms.vms.marketplace.shipment.repositories.IPackageRepository;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;

import static dk.ku.di.dms.vms.marketplace.shipment.ShipmentService.OLDEST_SHIPMENT_PER_SELLER;
import static java.lang.Thread.sleep;

public final class ShipmentTest {

    private static VmsApplication getVmsApplication() throws Exception {

        VmsApplicationOptions options = new VmsApplicationOptions("localhost", Constants.SHIPMENT_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.shipment",
                "dk.ku.di.dms.vms.marketplace.common"
        }, 4096, 2, 1000);

        return VmsApplication.build(options);
    }

    private static final BiFunction<Integer, String, CustomerCheckout> customerCheckoutBiFunction = (customerId, instanceId) -> new CustomerCheckout(
            customerId, "test", "test", "test", "test","test", "test", "test",
            "CREDIT_CARD","test","test","test", "test", "test", 1, instanceId);

    // generates an payment with orderId == sellerId
    private static final BiFunction<CustomerCheckout, Integer, PaymentConfirmed> paymentConfirmedBiFunction = (customerCheckout, orderId) -> new PaymentConfirmed(customerCheckout, orderId, 100f,
            List.of(new OrderItem(orderId,1,1, "name", orderId, 1.0f, new Date(), 1.0f, 1, 1.0f, 1.0f, 0.0f) ),
            new Date(), customerCheckout.instanceId);

    @Test
    public void testPackageQueryMultiVersionVisibility() throws Exception {
        VmsApplication vms = getVmsApplication();
        vms.start();

        for(int i = 1; i < 4; i++) {
            generatePaymentConfirmed(i, String.valueOf(i), i - 1, vms);
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
            generatePaymentConfirmed(i, String.valueOf(i), i - 1, vms);
        }

        sleep(5000);

        TransactionMetadata.registerTransactionStart( 11, 0, 10, true );

        List<OldestSellerPackageEntry> packages = packageRepository.query(
                OLDEST_SHIPMENT_PER_SELLER, OldestSellerPackageEntry.class);

        assert packages.size() == 10;
    }

    private static void generatePaymentConfirmed(int tid, String instanceId, int previousTid, VmsApplication vms) {
        CustomerCheckout customerCheckout = customerCheckoutBiFunction.apply(tid, instanceId);
        PaymentConfirmed paymentConfirmed = paymentConfirmedBiFunction.apply(customerCheckout, tid);

        InboundEvent inboundEvent = new InboundEvent(tid, previousTid, 1,
                "payment_confirmed", PaymentConfirmed.class, paymentConfirmed);
        vms.internalChannels().transactionInputQueue().add(inboundEvent);
    }

    /**
     * A single thread at the half, and end of batch to test correctness
     */
    @Test
    public void testMixedParallelSingleThreadTasks() throws Exception {
        VmsApplication vms = getVmsApplication();
        vms.start();

        int numPayments = 10;

        for(int i = 1; i <= numPayments; i++) {
            generatePaymentConfirmed(i, String.valueOf(i), i - 1, vms);
        }

        numPayments++;

        sleep(1000);

        InboundEvent updateShipment = new InboundEvent(numPayments, numPayments-1, 1,
                "update_shipment", String.class, String.valueOf(numPayments));
        vms.internalChannels().transactionInputQueue().add(updateShipment);

        sleep(1000);

        assert vms.lastTidFinished() == 11;

        numPayments = 20;

        for(int i = 12; i <= numPayments; i++) {
            generatePaymentConfirmed(i, String.valueOf(i), i - 1, vms);
        }

        numPayments++;

        updateShipment = new InboundEvent(numPayments, numPayments-1, 1,
                "update_shipment", String.class, String.valueOf(numPayments));
        vms.internalChannels().transactionInputQueue().add(updateShipment);

        sleep(1000);

        // not volatile, cant make sure
        assert vms.lastTidFinished() == 21;

    }

}
