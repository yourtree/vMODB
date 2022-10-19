package dk.ku.di.dms.vms.e_commerce.order;

import dk.ku.di.dms.vms.e_commerce.common.Item;
import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Entity;
import javax.persistence.IdClass;
import java.io.Serializable;

@Entity
@VmsTable(name="cart_items")
@IdClass(OrderItem.Id.class)
public class OrderItem implements IEntity<OrderItem.Id> {

    public static class Id implements Serializable {
        public long order_id;
        public long item_id;
        public Id(){}
    }

    @VmsForeignKey(table=Order.class, column = "id")
    public long cart_id;

    @VmsForeignKey(table= Item.class, column = "id")
    public long item_id;

}
