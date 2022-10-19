package dk.ku.di.dms.vms.e_commerce.common;

import dk.ku.di.dms.vms.modb.api.annotations.VmsReplica;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@VmsReplica(virtualMicroservice="user", table="user", columns = { "id", "firstName", "lastName", "username" })
public class Customer implements IEntity<Long> {

    @Id
    public long id;

    @Column
    public String firstName;

    @Column
    public String lastName;

    @Column
    public String username;



}