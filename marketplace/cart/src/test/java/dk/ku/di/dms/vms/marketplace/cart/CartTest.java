package dk.ku.di.dms.vms.marketplace.cart;

import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.cart.repositories.ICartItemRepository;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.Utils;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static dk.ku.di.dms.vms.marketplace.common.Constants.CUSTOMER_CHECKOUT;
import static java.lang.Thread.sleep;

public final class CartTest {

    @Test
    public void test() throws Exception {
        Properties properties = Utils.loadProperties();
        int networkBufferSize = Integer.parseInt(properties.getProperty("network_buffer_size"));
        int networkThreadPoolSize = Integer.parseInt(properties.getProperty("network_thread_pool_size"));

        VmsApplicationOptions options = new VmsApplicationOptions("localhost", Constants.CART_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.cart",
                "dk.ku.di.dms.vms.marketplace.common"
        }, networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize, networkThreadPoolSize);

        VmsApplication vms = VmsApplication.build(options);
        vms.start();

        var table = vms.getTable("cart_items");

        Object[] obj = new Object[]{
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

        IKey key = KeyUtils.buildRecordKey( table.schema().getPrimaryKeyColumns(), obj );
        table.underlyingPrimaryKeyIndex().insert(key, obj);

        // add to customer idx
        NonUniqueSecondaryIndex secIdx = table.secondaryIndexMap.get( KeyUtils.buildIndexKey( new int[]{2} ) );
        secIdx.insert( key, obj );

        CustomerCheckout customerCheckout = new CustomerCheckout(
                1, "test", "test", "test", "test","test", "test", "test",
                "CREDIT_CARD","test","test","test", "test", "test", 1,"1");

        InboundEvent inboundEvent = new InboundEvent( 1, 0, 1,
                CUSTOMER_CHECKOUT, CustomerCheckout.class, customerCheckout );
        vms.internalChannels().transactionInputQueue().add( inboundEvent );

        sleep(1000);

        assert vms.lastTidFinished() == 1;

        // register tid 2 and query the state of customer 1 cart items to see if they are deleted
        TransactionMetadata.registerTransactionStart( 2, 0, 1, true );

        ICartItemRepository cartItemRepository = (ICartItemRepository) vms.getRepositoryProxy("cart_items");
        List<CartItem> list = cartItemRepository.getCartItemsByCustomerId(1);

        assert list.size() == 0;
    }

}
