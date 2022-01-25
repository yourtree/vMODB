package dk.ku.di.dms.vms.eShopOnContainers.entity;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import java.util.List;

@Entity
@Table(name="discounts")
public class Discount extends AbstractEntity {

   public Discount() {

    }

    @ManyToMany
    @JoinColumn(name="checkout_id")
    private List<Checkout> checkouts;

}
