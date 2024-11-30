package dk.ku.di.dms.vms.tpcc.inventory.infra;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Item;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

public final class InventoryHttpHandler extends DefaultHttpHandler {

    private final IItemRepository itemRepository;

    private final IStockRepository stockRepository;

    public InventoryHttpHandler(ITransactionManager transactionManager,
                                IItemRepository itemRepository,
                                IStockRepository stockRepository) {
        super(transactionManager);
        this.itemRepository = itemRepository;
        this.stockRepository = stockRepository;
    }

    @Override
    public void post(String uri, String payload) {
        String[] uriSplit = uri.split("/");
        String table = uriSplit[uriSplit.length - 1];
        switch (table){
            case "item" -> {
                Item item = SERDES.deserialize(payload, Item.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.itemRepository.upsert(item);
            }
            case "stock" -> {
                Stock stock = SERDES.deserialize(payload, Stock.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.stockRepository.upsert(stock);
            }
        }
    }

}
