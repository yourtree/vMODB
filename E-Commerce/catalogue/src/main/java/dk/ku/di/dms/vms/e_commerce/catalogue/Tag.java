package dk.ku.di.dms.vms.e_commerce.catalogue;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;

@Entity
@VmsTable(name="tag")
public class Tag implements IEntity<Integer> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public int tag_id;

    @Column
    public String name;

}
