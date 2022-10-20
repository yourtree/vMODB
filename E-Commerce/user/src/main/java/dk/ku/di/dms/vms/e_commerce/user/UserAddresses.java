package dk.ku.di.dms.vms.e_commerce.user;

import dk.ku.di.dms.vms.e_commerce.common.entity.Address;
import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;

import javax.persistence.Entity;
import javax.persistence.IdClass;
import java.io.Serializable;

@Entity
@VmsTable(name="user_addresses")
@IdClass(UserAddresses.Id.class)
public class UserAddresses {

    public static class Id implements Serializable {
        public long user_id;
        public int address_id;
        public Id(){}
    }

    @VmsForeignKey(table=User.class, column = "id")
    public long user_id;

    @VmsForeignKey(table= Address.class, column = "id")
    public int address_id;

}
