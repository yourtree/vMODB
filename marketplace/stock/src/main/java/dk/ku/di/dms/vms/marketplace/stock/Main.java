package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.stock.infra.StockDbUtils;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.lang.System.Logger;
import java.util.List;
import java.util.Properties;

public final class Main {

    private static final Logger LOGGER = System.getLogger(Main.class.getName());

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
                Constants.STOCK_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.marketplace.stock",
                        "dk.ku.di.dms.vms.marketplace.common"
                });
        return VmsApplication.build(options,
                (x,y) -> new StockHttpHandlerJdk2(x, (IStockRepository) y.apply("stock_items")));
    }
    
    private static class StockHttpHandlerJdk2 implements IHttpHandler {

        private final ITransactionManager transactionManager;
        private final IStockRepository repository;

        public StockHttpHandlerJdk2(ITransactionManager transactionManager,
                                     IStockRepository repository){
            this.transactionManager = transactionManager;
            this.repository = repository;
        }

        @Override
        public void post(String uri, String body) {
            StockItem stockItem = StockDbUtils.deserializeStockItem(body);
            this.transactionManager.beginTransaction(0, 0, 0, false);
            this.repository.upsert(stockItem);
        }

        @Override
        public void patch(String uri, String body) {
            final String[] uriSplit = uri.split("/");
            String op = uriSplit[uriSplit.length - 1];
            if(op.contentEquals("reset")){
                // path: /stock/reset
                this.transactionManager.reset();
                return;
            }
            this.transactionManager.beginTransaction(0, 0, 0,false);
            List<StockItem> stockItems = this.repository.getAll();
            for(StockItem item : stockItems){
                item.qty_available = 10000;
                item.version = "0";
                item.qty_reserved = 0;
                this.repository.upsert(item);
            }
        }

        @Override
        public String getAsJson(String uri) {
            final String[] uriSplit = uri.split("/");
            int sellerId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
            int productId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
            this.transactionManager.beginTransaction(0, 0, 0, true);
            StockItem stockItem = this.repository.lookupByKey(new StockItem.StockId(sellerId, productId));
            return stockItem.toString();
        }
    }

}