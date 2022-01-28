package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.infra.AbstractEntityDefault;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

@Entity
@Table(name="products")
public class Product extends AbstractEntityDefault {

    @Column
    private Double price;

    @Column
    private String description;

    @Column
    private String name;

    public Product() { }

    public Product(@NotNull Double price, String description, String name) {
        this.price = price;
        this.description = description;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Product{" +
                "id=" + id +
                ", price=" + price +
                ", description='" + description + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
