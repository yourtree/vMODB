package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import java.util.List;

@Entity
@Table(name="customers")
public class Customers extends AbstractEntity {

    public Customers() {

    }

    @ManyToMany
    @JoinColumn(name="checkout_id")
    private List<Checkout> checkouts;

}
