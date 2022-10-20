package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

import java.util.List;

@Repository
public interface ICartRepository extends IRepository<Long, Cart> {

    @Query("select * from carts where customerId = :customerId AND sealed = true")
    Cart findByCustomerId(Long customerId);

}
