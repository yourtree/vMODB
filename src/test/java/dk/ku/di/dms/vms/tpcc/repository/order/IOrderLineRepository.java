package dk.ku.di.dms.vms.tpcc.repository.order;

import dk.ku.di.dms.vms.annotations.Repository;
import dk.ku.di.dms.vms.infra.IRepository;
import dk.ku.di.dms.vms.tpcc.entity.OrderLine;

@Repository
public interface IOrderLineRepository extends IRepository<OrderLine.OrderLineId, OrderLine> {


}
