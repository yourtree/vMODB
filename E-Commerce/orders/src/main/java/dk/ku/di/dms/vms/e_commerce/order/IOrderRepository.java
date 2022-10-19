package dk.ku.di.dms.vms.e_commerce.order;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

import java.util.List;

@Repository
public interface IOrderRepository extends IRepository<Long, Order> {

    List<Order> findByCustomerId(long customerId);

}