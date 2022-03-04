package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.infra.AbstractEntityDefault;

import javax.persistence.*;
import java.util.List;

@Entity
@Table(name="carts")
public class Cart extends AbstractEntityDefault {

    @ManyToOne
    @JoinColumn(name = "customer_id")
    private Customers customer;

    @OneToMany
    @JoinColumn(name="cart_item_id")
    private List<CartItem> items;

}
