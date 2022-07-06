package dk.ku.di.dms.vms.eshop.entity;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.sdk.core.annotations.VmsTable;

import javax.persistence.*;
import java.util.List;

@Entity
@VmsTable(name="customers")
public class Customers implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public Long id;

    @ManyToMany
    @JoinColumn(name="checkout_id")
    private List<Checkout> checkouts;

}
