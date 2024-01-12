package dk.ku.di.dms.vms.marketplace.order.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.util.Date;

@Entity
@VmsTable(name="order_history")
public class OrderHistory {

    @Id
    @GeneratedValue
    public int id;

    @VmsForeignKey( table = Order.class, column = "id")
    public int order_id;

    @Column
    public Date created_at;

    @Column
    public OrderStatus status;

    public OrderHistory() { }

}
