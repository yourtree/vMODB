package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;

@Entity
@Table(name="warehouse")
public class Warehouse extends AbstractEntity<Integer> {

    @Id
    @GeneratedValue
    public Integer w_id;

    @Column
    public Float w_tax;

    @Column
    public Float w_ytd;

    /*
    w_id smallint not null,
	w_name varchar(10),
	w_street_1 varchar(20),
	w_street_2 varchar(20),
	w_city varchar(20),
	w_state char(2),
	w_zip char(9),
	w_tax decimal(4,2),
	w_ytd decimal(12,2),
	primary key (w_id)
 */

    public Warehouse(){}

}
