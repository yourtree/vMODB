package dk.ku.di.dms.vms.e_commerce.user;

import dk.ku.di.dms.vms.e_commerce.common.Address;
import dk.ku.di.dms.vms.e_commerce.common.Card;
import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;

@Entity
@VmsTable(name="user")
public class User implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public long id;

    @Column
    public String firstName;

    @Column
    public String lastName;

    @Column
    public String email;

    @Column
    public String username;

    @Column
    public String password;

    // TODO originally these are 1:N. now it is 1:1

    @VmsForeignKey(table= Address.class, column = "id")
    public int address_id;

    @VmsForeignKey(table= Card.class, column = "id")
    public int card_id;

    @Column
    public String userId;

    @Column
    public String salt;

}
