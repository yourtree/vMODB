package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.modb.api.annotations.ExternalVmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;

@Entity
@VmsTable(name="carts")
public class Cart implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public long id;

    @ExternalVmsForeignKey(vms="user", column = "id")
    private long customerId;

    @Column
    private boolean sealed;

}