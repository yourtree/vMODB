package dk.ku.di.dms.vms.marketplace.cart.repositories;

import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

import java.util.List;

public interface ICartItemRepository extends IRepository<CartItem.CartItemId, CartItem> {

    @Query("select * from carts where customer_id = :customerId")
    List<CartItem> getCartItemsByCustomerId(int customerId);

}
