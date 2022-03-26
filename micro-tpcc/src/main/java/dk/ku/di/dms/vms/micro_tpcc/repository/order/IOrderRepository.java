package dk.ku.di.dms.vms.micro_tpcc.repository.order;

import dk.ku.di.dms.vms.sdk.core.annotations.Repository;
import dk.ku.di.dms.vms.micro_tpcc.entity.Order;
import dk.ku.di.dms.vms.modb.common.interfaces.IRepository;

@Repository
public interface IOrderRepository extends IRepository<Order.OrderId, Order> {


}
