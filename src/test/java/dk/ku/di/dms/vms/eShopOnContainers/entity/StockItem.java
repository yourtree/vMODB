package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.eShopOnContainers.entity.Product;

import javax.persistence.*;
import javax.validation.constraints.Min;

@Entity
@Table(name="stock_item")
public class StockItem {

    @Id
    private Long id;

    @OneToOne
    @JoinColumn(name="product_id")
    private Product product;

    @Min(0)
    private int available;

    public Long getId() {
        return id;
    }

    public int getAvailable() {
        return available;
    }

    public void decrease(){
        this.available = available - 1;
    }

}
