package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.annotations.VmsTable;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import java.util.List;

@Entity
@VmsTable(name="discounts")
public class Discount extends AbstractEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public Long id;

    @ManyToMany
    @JoinColumn(name="checkout_id")
    private List<Checkout> checkouts;

    public Discount() {

    }

}
