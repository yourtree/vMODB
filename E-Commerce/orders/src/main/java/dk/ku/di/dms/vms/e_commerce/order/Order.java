package dk.ku.di.dms.vms.e_commerce.order;

import dk.ku.di.dms.vms.e_commerce.common.Address;
import dk.ku.di.dms.vms.e_commerce.common.Card;
import dk.ku.di.dms.vms.e_commerce.common.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import java.util.Date;

@Entity
@VmsTable(name="order")
public class Order implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    public long id;

    @VmsForeignKey(table= Customer.class, column = "id")
    public long customerId;

    @VmsForeignKey(table= Address.class, column = "id")
    public int addressId;

    @VmsForeignKey(table= Card.class, column = "id")
    public int cardId;

    @Column
    public Date date;

    @Column
    private float total;

}
