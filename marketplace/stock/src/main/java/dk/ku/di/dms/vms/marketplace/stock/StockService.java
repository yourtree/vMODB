package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdatedEvent;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.events.StockConfirmed;
import dk.ku.di.dms.vms.marketplace.common.events.TransactionMark;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("stock")
public class StockService {

    private final IStockRepository stockRepository;

    public StockService(IStockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @Inbound(values = {"product_updated"})
    @Outbound("transaction_mark")
    @Transactional(type=RW)
    public TransactionMark updateProduct(ProductUpdatedEvent updateEvent) {
        System.out.println("Stock received an update product event with version: "+updateEvent.version);

        // can use issue statement for faster update
        Stock stock = stockRepository.lookupByKey(new Stock.StockId(updateEvent.sellerId, updateEvent.productId));

        stock.version = updateEvent.version;

        stockRepository.update(stock);

        return new TransactionMark( updateEvent.version, TransactionMark.TransactionType.UPDATE_PRODUCT,
                updateEvent.sellerId, TransactionMark.MarkStatus.SUCCESS, "stock");
    }

    @Inbound(values = {"reserve_stock"})
    @Outbound("stock_confirmed")
    @Transactional(type=RW)
    public StockConfirmed reserveStock(ReserveStock reserveStock){

        List<Stock.StockId> listOfIds = reserveStock.items.stream().map(f -> new Stock.StockId(f.SellerId, f.ProductId)).toList();

        // TODO finish logic
        List<Stock> items = stockRepository.lookupByKeys(listOfIds);

        return new StockConfirmed( reserveStock.timestamp, reserveStock.customerCheckout, reserveStock.items, reserveStock.instanceId );

    }

}
