package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IStockRepository extends IRepository<Stock.StockId, Stock> {



}
