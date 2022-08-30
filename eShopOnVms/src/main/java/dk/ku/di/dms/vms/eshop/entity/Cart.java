package dk.ku.di.dms.vms.eshop.entity;

import dk.ku.di.dms.vms.modb.common.interfaces.application.IEntity;
import dk.ku.di.dms.vms.sdk.core.annotations.VmsTable;

import javax.persistence.*;
import java.util.List;

@Entity
@VmsTable(name="carts")
public class Cart implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public Long id;

    @ManyToOne
    @JoinColumn(name = "customer_id")
    private Customers customer;

    @OneToMany
    @JoinColumn(name="cart_item_id")
    private List<CartItem> items;

}
