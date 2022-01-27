package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;

@Entity
@Table(name="item")
public class Item extends AbstractEntity<Integer> {

    @Id
    @GeneratedValue
    public Integer i_id;

    @Column
    public Float i_price;

    @Column
    public String i_name;

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

}
