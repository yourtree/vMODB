package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.events.StockConfirmed;
import dk.ku.di.dms.vms.marketplace.common.events.TransactionMark;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("stock")
public class StockService {

    private static final Logger LOGGER = Logger.getLogger(StockService.class.getCanonicalName());

    private final IStockRepository stockRepository;

    public StockService(IStockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @Inbound(values = {"product_updated"})
    @Outbound("transaction_mark")
    @Transactional(type=RW)
    public TransactionMark updateProduct(ProductUpdated updateEvent) {
        System.out.println("Stock received an update product event with version: "+updateEvent.version);

        // can use issue statement for faster update
        StockItem stock = this.stockRepository.lookupByKey(new StockItem.StockId(updateEvent.sellerId, updateEvent.productId));

        stock.version = updateEvent.version;

        this.stockRepository.update(stock);

        return new TransactionMark( updateEvent.version, TransactionMark.TransactionType.UPDATE_PRODUCT,
                updateEvent.sellerId, TransactionMark.MarkStatus.SUCCESS, "stock");
    }

    @Inbound(values = {"reserve_stock"})
    @Outbound("stock_confirmed")
    @Transactional(type=RW)
    public StockConfirmed reserveStock(ReserveStock reserveStock){

        System.out.println("Stock received a reserve stock event with TID: "+reserveStock.instanceId);

        Map<StockItem.StockId, CartItem> cartItemMap = reserveStock.items.stream().collect( Collectors.toMap( (f)-> new StockItem.StockId(f.SellerId, f.ProductId), Function.identity()) );

        List<StockItem> items = stockRepository.lookupByKeys(cartItemMap.keySet());

        if(items.isEmpty()) {
            LOGGER.severe("No items found in private state");
            return null;
        }

        // List<CartItem> unavailableItems = new();
        List<CartItem> cartItemsReserved = new ArrayList<>(reserveStock.items.size());
//        List<StockItem> stockItemsReserved = new(); // for bulk update
        var now = new Date();
        StockItem.StockId currId;
        for(StockItem item : items){

            currId = new StockItem.StockId( item.seller_id, item.product_id );

            CartItem cartItem = cartItemMap.get( currId );

            if (item.version.compareTo(cartItem.Version) != 0) {
                LOGGER.warning("The version is incorrect. Stock item: "+ item.version+ " Cart item: "+cartItem.Version);
                continue;
            }

            if(item.qty_reserved + cartItem.Quantity > item.qty_available){
                continue;
            }

            item.qty_reserved += cartItem.Quantity;
            item.updated_at = now;

            this.stockRepository.update( item );

            cartItemsReserved.add(cartItem);

        }

        return new StockConfirmed( reserveStock.timestamp, reserveStock.customerCheckout, cartItemsReserved, reserveStock.instanceId );

    }

}
