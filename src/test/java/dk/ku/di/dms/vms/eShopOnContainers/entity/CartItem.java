package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.annotations.VmsTable;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import javax.validation.constraints.PositiveOrZero;

@Entity
@VmsTable(name="cart_items")
public class CartItem extends AbstractEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public Long id;

    @ManyToOne
    @JoinColumn(name = "product_id")
    private Product product;

    @Column
    private Float price;

    @Column
    @PositiveOrZero
    private int qtd;

    @ManyToOne
    @JoinColumn(name = "cart_id")
    private Cart cart;

    public CartItem() {

    }

    public CartItem(Product product, int qtd) {
        this.product = product;
//        this.price = price;
        this.qtd = qtd;
    }

}
