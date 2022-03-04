package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.infra.AbstractEntityDefault;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

@Entity
@Table(name="products",
        indexes = @Index(name = "uniqueSkuIndex", columnList = "sku", unique = true)
)
public class Product extends AbstractEntityDefault {

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
