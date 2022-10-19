package dk.ku.di.dms.vms.micro_tpcc.common.entity;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;

import javax.persistence.*;

@Entity
@VmsTable(name="warehouse")
public class Warehouse implements IEntity<Integer> {

    @Id
    @GeneratedValue
    public int w_id;

    @Column
    public double w_tax;

    @Column
    public double w_ytd;

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

    public Warehouse(int w_id, double w_tax, double w_ytd) {
        this.w_id = w_id;
        this.w_tax = w_tax;
        this.w_ytd = w_ytd;
    }

}
