package dk.ku.di.dms.vms.e_commerce.catalogue;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.*;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

@Entity
@VmsTable(name="product")
public class Product implements IEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    public long id;

    @Column
    public String name;

    @Column
    public String description;

    @Column
    @Positive
    public float price;

    @Column
    @PositiveOrZero
    public int count;

    @Column
    public String image_url_1;

    @Column
    public String image_url_2;

    @VmsForeignKey(table= SockTag.class, column = "tag_id")
    public int tag_id;

    public String tagName;

}
