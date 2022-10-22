package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@VmsTable(name="cart_items")
public class CartItem implements IEntity<Long> {

    // @ExternalVmsForeignKey(table= Product.class, column = "id")
    @Id
    public long productId;

    @Column
    public int quantity;

    @Column
    public float unitPrice;

    // TODO create a secondary index, non-unique hash. why? because it is a composite primary key, may have many cart_id of the same value
    @VmsForeignKey(table=Cart.class, column = "id")
    public long cartId;

}
