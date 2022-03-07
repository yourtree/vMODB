package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.annotations.VmsTable;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import javax.validation.constraints.Min;

@Entity
@VmsTable(name="stock_item")
public class StockItem extends AbstractEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public Long id;

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
