package dk.ku.di.dms.vms.e_commerce.order;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;

@Entity
@VmsTable(name="order_items")
public class OrderItem implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public long id;

    @Column
    public int quantity;

    @Column
    public float unitPrice;

    @VmsForeignKey(table=Order.class, column = "id")
    public long order_id;

    public OrderItem(int quantity, float unitPrice, long order_id) {
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.order_id = order_id;
    }

}
