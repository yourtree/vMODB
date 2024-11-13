package dk.ku.di.dms.vms.tpcc.order.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;

@Repository
public interface IOrderLineRepository extends IRepository<OrderLine.OrderLineId, OrderLine> { }