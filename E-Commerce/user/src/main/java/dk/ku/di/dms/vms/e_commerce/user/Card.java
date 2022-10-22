package dk.ku.di.dms.vms.e_commerce.user;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import java.util.Date;

@Entity
@VmsTable(name="card")
public class Card implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public long id;

    @Column
    public String longNum;

    @Column
    public Date expires;

    @Column
    public String ccv;

    @VmsForeignKey(table = User.class, column = "id")
    public long userId;

}
