package dk.ku.di.dms.vms.marketplace.order.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@VmsTable(name="customer_orders")
public class CustomerOrder {

    @Id
    public int customer_id;

    @Column
    public int next_order_id;

    public CustomerOrder() { }

}
