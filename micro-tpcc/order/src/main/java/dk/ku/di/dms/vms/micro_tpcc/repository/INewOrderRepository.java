package dk.ku.di.dms.vms.micro_tpcc.repository;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.micro_tpcc.entity.NewOrder;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface INewOrderRepository extends IRepository<NewOrder.NewOrderId, NewOrder> {


}
