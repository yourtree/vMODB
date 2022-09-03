package dk.ku.di.dms.vms.micro_tpcc.entity;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.sdk.core.annotations.VmsTable;

import javax.persistence.*;

@Entity
@VmsTable(name="item")
public class Item implements IEntity<Integer> {

    @Id
    @GeneratedValue
    public int i_id;

    @Column
    public int i_im_id;

    @Column
    public String i_name;

    @Column
    public float i_price;

    @Column
    public String i_data;

    /*
    i_id int not null,
	i_im_id int,
	i_name varchar(24),
	i_price decimal(5,2),
	i_data varchar(50),
	PRIMARY KEY(i_id)
     */

    public Item(){}

    public Item(int i_id, int i_im_id, String i_name, float i_price, String i_data) {
        this.i_id = i_id;
        this.i_im_id = i_im_id;
        this.i_name = i_name;
        this.i_price = i_price;
        this.i_data = i_data;
    }

}
