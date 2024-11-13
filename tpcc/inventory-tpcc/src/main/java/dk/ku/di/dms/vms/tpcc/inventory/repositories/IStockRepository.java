package dk.ku.di.dms.vms.tpcc.inventory.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;

@Repository
public interface IStockRepository extends IRepository<Stock.StockId, Stock> {

}