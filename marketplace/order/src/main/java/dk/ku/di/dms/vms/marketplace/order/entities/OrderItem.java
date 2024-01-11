package dk.ku.di.dms.vms.marketplace.order.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Date;

@Entity
@VmsTable(name="order_items")
public class OrderItem implements IEntity<OrderItem.OrderItemId> {

    public static class OrderItemId implements Serializable {
        public int order_id;
        public int order_item_id;

        public OrderItemId(){}

        public OrderItemId(int order_id, int order_item_id) {
            this.order_id = order_id;
            this.order_item_id = order_item_id;
        }
    }

    @Id
    @VmsForeignKey(table = Order.class, column = "id")
    public int order_id;

    @Id
    public int order_item_id;

    @Column
    public int product_id;

    @Column
    public String product_name;

    @Column
    public int seller_id;

    @Column
    public float unit_price;

    @Column
    public Date shipping_limit_date;

    @Column
    public float freight_value;

    @Column
    public int quantity;

    @Column
    public float total_items;

    @Column
    public float total_amount;

    // can be derived from total_items - total_amount
    // incentive of item is not of concern to the order
    // the seller must compute 
    // public float total_incentive;

    public OrderItem() { }
    
}
