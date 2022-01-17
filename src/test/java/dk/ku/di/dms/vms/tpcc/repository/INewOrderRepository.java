package dk.ku.di.dms.vms.tpcc.repository;

import dk.ku.di.dms.vms.annotations.Repository;
import dk.ku.di.dms.vms.infra.IRepository;
import dk.ku.di.dms.vms.tpcc.entity.NewOrder;

@Repository
public interface INewOrderRepository extends IRepository<NewOrder.NewOrderId, NewOrder> {


}
