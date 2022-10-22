package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;

@Entity
@VmsTable(name="carts")
public class Cart implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public long id;

    // @ExternalVmsForeignKey(table= User.class, column = "id")
    @Column
    public long customerId;

    @Column
    public boolean sealed;

}