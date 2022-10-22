package dk.ku.di.dms.vms.e_commerce.catalogue;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@VmsTable(name="sock_tag")
@IdClass(SockTag.Id.class)
public class SockTag implements IEntity<SockTag.Id> {

    public static class Id implements Serializable {
        public long product_id;
        public int tag_id;
        public Id(){}
    }

    @VmsForeignKey(table= Product.class, column = "id")
    public long product_id;

    @VmsForeignKey(table=Tag.class, column = "tag_id")
    public int tag_id;

}
