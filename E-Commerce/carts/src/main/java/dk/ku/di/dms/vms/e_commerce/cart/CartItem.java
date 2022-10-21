package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.e_commerce.common.entity.Item;
import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@VmsTable(name="cart_items")
@IdClass(CartItem.Id.class)
public class CartItem implements IEntity<CartItem.Id> {

    public static class Id implements Serializable {
        public long cart_id;
        public long item_id;
        public Id(){}
    }

    // TODO create a secondary index, non unique hash. why? because it is a composite primary key, may have many cart_id of the same value
    @VmsForeignKey(table=Cart.class, column = "id")
    public long cart_id;

    @VmsForeignKey(table= Item.class, column = "id")
    public long item_id;

}
