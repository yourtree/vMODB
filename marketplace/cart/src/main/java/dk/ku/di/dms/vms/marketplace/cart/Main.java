package dk.ku.di.dms.vms.marketplace.cart;

import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.cart.infra.CartUtils;
import dk.ku.di.dms.vms.marketplace.cart.repositories.ICartItemRepository;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.util.List;
import java.util.Properties;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] ignoredArgs) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        try(VmsApplication vms = buildVms(properties)){
            vms.start();
        }
    }

    private static VmsApplication buildVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.CART_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.cart",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        return VmsApplication.build(options,
                (x,z) -> new CartHttpHandlerJdk2(x, (ICartItemRepository) z.apply("cart_items")));
    }
    
    private static class CartHttpHandlerJdk2 implements IHttpHandler {

        private final ITransactionManager transactionManager;
        private final ICartItemRepository repository;
        private static final IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();

        public CartHttpHandlerJdk2(ITransactionManager transactionManager,
                                   ICartItemRepository repository){
            this.transactionManager = transactionManager;
            this.repository = repository;
        }

        @Override
        public void patch(String uri, String body) {
            String[] split = uri.split("/");
            String op = split[split.length - 1];
            if(op.contentEquals("add")) {
                int customerId = Integer.parseInt(split[split.length - 2]);
                dk.ku.di.dms.vms.marketplace.common.entities.CartItem cartItemAPI =
                        SERDES.deserialize(body, dk.ku.di.dms.vms.marketplace.common.entities.CartItem.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.repository.insert(CartUtils.convertCartItemAPI(customerId, cartItemAPI));
                return;
            }
            this.transactionManager.reset();
        }

        @Override
        public String getAsJson(String uri) {
            String[] split = uri.split("/");
            int customerId = Integer.parseInt(split[split.length - 1]);
            this.transactionManager.beginTransaction( 0, 0, 0,true );
            List<CartItem> cartItems = this.repository.getCartItemsByCustomerId(customerId);
            return SERDES.serializeList(cartItems);

        }
    }

}
