package dk.ku.di.dms.vms.micro_tpcc.order.repository;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.micro_tpcc.order.entity.Order;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IOrderRepository extends IRepository<Order.OrderId, Order> {


}
