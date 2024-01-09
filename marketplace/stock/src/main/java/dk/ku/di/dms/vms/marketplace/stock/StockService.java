package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.marketplace.common.ProductUpdatedEvent;
import dk.ku.di.dms.vms.marketplace.common.TransactionMark;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

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
        System.out.println("Stock received an update product event");

        // can use issue statement for faster update
        Stock stock = stockRepository.lookupByKey(new Stock.StockId(updateEvent.sellerId, updateEvent.productId));

        stock.version = updateEvent.version;

        stockRepository.update(stock);

        return new TransactionMark( updateEvent.version, TransactionMark.TransactionType.UPDATE_PRODUCT,
                updateEvent.sellerId, TransactionMark.MarkStatus.SUCCESS, "stock");
    }

}
