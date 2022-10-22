package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

import java.util.List;

@Repository
public interface ICartItemRepository extends IRepository<Long, CartItem> {

    @Query("select cart_items.* from carts, cart_items where customerId = :customerId AND sealed = false " +
            "AND cart_items.cartId = carts.id")
    List<CartItem> findOpenCartByCustomerId(Long customerId);

}
