package dk.ku.di.dms.vms.micro_tpcc.order.repository;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.micro_tpcc.order.entity.NewOrder;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface INewOrderRepository extends IRepository<NewOrder.NewOrderId, NewOrder> {


}
