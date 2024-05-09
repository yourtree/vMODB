package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.events.StockConfirmed;
import dk.ku.di.dms.vms.modb.api.annotations.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("stock")
public final class StockService {

    private static final Logger LOGGER = Logger.getLogger(StockService.class.getCanonicalName());

    private final IStockRepository stockRepository;

    public StockService(IStockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @Inbound(values = {PRODUCT_UPDATED})
    @Transactional(type=RW)
    @PartitionBy(clazz = ProductUpdated.class, method = "getId")
    public void updateProduct(ProductUpdated updateEvent) {
        System.out.println("APP: Stock received an update product event with version: "+updateEvent.version);

        // can use issue statement for faster update
        StockItem stock = this.stockRepository.lookupByKey(new StockItem.StockId(updateEvent.seller_id, updateEvent.product_id));

        stock.version = updateEvent.version;

        this.stockRepository.update(stock);
    }

    /**
     * This case can be optimized by locking the items prior to starting the transaction
     * The task is only submitted to run if all locks have been acquired. This prevents possible conflicts.
     */
    @Inbound(values = {RESERVE_STOCK})
    @Outbound(STOCK_CONFIRMED)
    @Transactional(type=RW)
    public StockConfirmed reserveStock(ReserveStock reserveStock){

        System.out.println("APP: Stock received a reserve stock event with TID: "+reserveStock.instanceId);

        Map<StockItem.StockId, CartItem> cartItemMap = reserveStock.items.stream().collect( Collectors.toMap( (f)-> new StockItem.StockId(f.SellerId, f.ProductId), Function.identity()) );

        List<StockItem> items = this.stockRepository.lookupByKeys(cartItemMap.keySet());

        if(items.isEmpty()) {
            throw new RuntimeException("No items found in private state");
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
