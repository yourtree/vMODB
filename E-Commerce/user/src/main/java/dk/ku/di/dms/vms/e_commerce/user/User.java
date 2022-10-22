package dk.ku.di.dms.vms.e_commerce.user;

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
    public String userId;

    @Column
    public String firstName;

    @Column
    public String lastName;

    @Column
    public String username;

    @Column
    public String salt;

}
