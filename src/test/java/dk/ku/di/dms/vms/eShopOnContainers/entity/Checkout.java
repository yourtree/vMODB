package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import java.util.List;

@Entity
@Table(name="checkouts")
public class Checkout extends AbstractEntity {

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
