package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.annotations.VmsIndex;
import dk.ku.di.dms.vms.annotations.VmsTable;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

@Entity
@VmsTable(name="products",
        indexes = {@VmsIndex(name = "uniqueSkuIndex", columnList = "sku", unique = true),
                @VmsIndex(name="testCompositeIndex", columnList = "sku,price"),
                @VmsIndex(name = "rangePriceIndex", columnList = "price", range = true)
        }
)
public class Product extends AbstractEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public Long id;

    @Column
    private Double price;

    @Column
    private String sku;

    @Column
    private String description;

    @Column
    private String name;

    public Product() { }

    public Product(@NotNull Double price, String description, String sku, String name) {
        this.price = price;
        this.description = description;
        this.sku = sku;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Product{" +
                "id=" + id +
                ", price=" + price +
                ", description='" + description + '\'' +
                ", sku='" + sku + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
