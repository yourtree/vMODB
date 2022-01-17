package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.infra.PersistentEntity;

import javax.persistence.*;

@Entity
@Table(name="cart_items")
public class CartItem extends PersistentEntity {

    @ManyToOne
    @JoinColumn(name = "product_id")
    private Product product;

    @Column
    private Float price;

    @Column
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
