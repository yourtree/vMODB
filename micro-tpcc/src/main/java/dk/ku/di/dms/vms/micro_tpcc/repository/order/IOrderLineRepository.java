package dk.ku.di.dms.vms.micro_tpcc.repository.order;

import dk.ku.di.dms.vms.sdk.core.annotations.Repository;
import dk.ku.di.dms.vms.micro_tpcc.entity.OrderLine;
import dk.ku.di.dms.vms.modb.common.interfaces.application.IRepository;

@Repository
public interface IOrderLineRepository extends IRepository<OrderLine.OrderLineId, OrderLine> {


}
