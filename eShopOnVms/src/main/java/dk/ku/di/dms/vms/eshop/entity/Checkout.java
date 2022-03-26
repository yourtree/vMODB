package dk.ku.di.dms.vms.eshop.entity;

import dk.ku.di.dms.vms.modb.common.IEntity;
import dk.ku.di.dms.vms.sdk.core.annotations.VmsTable;

import javax.persistence.*;
import java.util.List;

@Entity
@VmsTable(name="checkouts")
public class Checkout implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public Long id;

    @ManyToMany
    private List<Discount> discounts;

    @ManyToOne
    @JoinColumn(name = "customer_id")
    private Customers customer;

    @OneToMany
    @JoinColumn(name="cart_item_id")
    private List<CartItem> cartItems;

    public Checkout(List<Discount> discounts) {
        this.discounts = discounts;
    }

    public Checkout() {

    }

    public List<Discount> getDiscounts() {
        return discounts;
    }

}
