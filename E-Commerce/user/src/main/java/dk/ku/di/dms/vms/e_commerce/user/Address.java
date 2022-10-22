package dk.ku.di.dms.vms.e_commerce.user;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;

@Entity
@VmsTable(name="address")
public class Address implements IEntity<Long>  {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public long id;

    @Column
    public String street;

    @Column
    public String number;

    @Column
    public String country;

    @Column
    public String city;

    @Column
    public String postCode;

    @VmsForeignKey(table = User.class, column = "id")
    public long userId;

}
