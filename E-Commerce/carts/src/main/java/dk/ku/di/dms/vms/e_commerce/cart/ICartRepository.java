package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

import java.util.List;

@Repository
public interface ICartRepository extends IRepository<Long, Cart> {

    List<Cart> findByCustomerId(String customerId);

}
