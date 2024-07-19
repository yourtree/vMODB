package dk.ku.di.dms.vms.marketplace.cart;

import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.cart.repositories.ICartItemRepository;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.transaction.TransactionManager;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import org.junit.Test;

import java.util.List;

import static dk.ku.di.dms.vms.marketplace.common.Constants.CUSTOMER_CHECKOUT;
import static java.lang.Thread.sleep;

public final class CartTest {

    @Test
    public void test() throws Exception {
        VmsApplication vms = loadCartVms();

        Object[] cartItemRow = new Object[]{
                1,
                1,
                1,
                "test",
                10,
                10,
                1,
                0,
                "0"
        };

        insertCartItem(vms, cartItemRow);

        CustomerCheckout customerCheckout = new CustomerCheckout(
                1, "test", "test", "test", "test","test", "test", "test",
                "CREDIT_CARD","test","test","test", "test", "test", 1,"1");

        InboundEvent inboundEvent = new InboundEvent( 1, 0, 1,
                CUSTOMER_CHECKOUT, CustomerCheckout.class, customerCheckout );
        vms.internalChannels().transactionInputQueue().add( inboundEvent );

        sleep(1000);

        assert vms.lastTidFinished() == 1;

        // register tid 2 and query the state of customer 1 cart items to see if they are deleted
        vms.getTransactionManager().beginTransaction( 2, 0, 1, true );

        ICartItemRepository cartItemRepository = (ICartItemRepository) vms.getRepositoryProxy("cart_items");
        List<CartItem> list = cartItemRepository.getCartItemsByCustomerId(1);

        assert list.isEmpty();
    }

    private static void insertCartItem(VmsApplication vms, Object[] cartItemRow) {
        var table = vms.getTable("cart_items");

        vms.getTransactionManager()
                .beginTransaction( 0, 0, -1, false );
        var txCtx = ((TransactionManager)vms.getTransactionManager()).getTransactionContext();

        IKey key = KeyUtils.buildRecordKey( table.schema().getPrimaryKeyColumns(), cartItemRow );
        table.primaryKeyIndex().insert(txCtx, key, cartItemRow);

        // add to customer idx
        NonUniqueSecondaryIndex secIdx = table.secondaryIndexMap.get( KeyUtils.buildIndexKey( new int[]{2} ) );
        secIdx.insert(txCtx, key, cartItemRow);
    }

    private static VmsApplication loadCartVms() throws Exception {

        VmsApplicationOptions options = VmsApplicationOptions.build(
                "localhost",
                Constants.CART_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.cart",
                "dk.ku.di.dms.vms.marketplace.common"
        });

        VmsApplication vms = VmsApplication.build(options);
        vms.start();
        return vms;
    }

    @Test
    public void testTwoCustomerUpdates() throws Exception {
        VmsApplication vms = loadCartVms();

        // cart item for same customer id
        Object[] cartItemRow = new Object[]{
                1,
                1,
                1,
                "test",
                10,
                10,
                1,
                0,
                "0"
        };



        for(int i = 1; i <= 2; i++) {
            vms.getTransactionManager().beginTransaction(i-1, 0,i-1,false);

            System.out.println("Adding item " + i + " to the cart");
            insertCartItem(vms, cartItemRow);
            CustomerCheckout customerCheckout = new CustomerCheckout(
                    1, "test", "test", "test", "test", "test", "test", "test",
                    "CREDIT_CARD", "test", "test", "test", "test", "test", 1, String.valueOf(i));
            InboundEvent inboundEvent = new InboundEvent(i, i-1, 1,
                    CUSTOMER_CHECKOUT, CustomerCheckout.class, customerCheckout);
            vms.internalChannels().transactionInputQueue().add(inboundEvent);

            // have to wait until the first transaction finishes in order to add
            // the new cart item and avoid deleting them all before the second transaction starts
            // this causes blocking the test
            // while(vms.lastTidFinished() != i);
            sleep(2000);
        }

//        sleep(2000);

        assert vms.lastTidFinished() == 2;

        // register tid 2 and query the state of customer 1 cart items to see if they are deleted
        vms.getTransactionManager().beginTransaction( 3, 0, 2, true );

        ICartItemRepository cartItemRepository = (ICartItemRepository) vms.getRepositoryProxy("cart_items");
        List<CartItem> list = cartItemRepository.getCartItemsByCustomerId(1);

        assert list.isEmpty();

    }

}
