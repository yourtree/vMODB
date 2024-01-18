package dk.ku.di.dms.vms.marketplace.order.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@VmsTable(name="customer_orders")
public class CustomerOrder implements IEntity<Integer> {

    @Id
    public int customer_id;

    @Column
    public int next_order_id;

    public CustomerOrder() { }

    public CustomerOrder(int customer_id, int next_order_id) {
        this.customer_id = customer_id;
        this.next_order_id = next_order_id;
    }

}
