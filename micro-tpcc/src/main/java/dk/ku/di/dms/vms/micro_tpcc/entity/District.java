package dk.ku.di.dms.vms.micro_tpcc.entity;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.sdk.core.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.sdk.core.annotations.VmsTable;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@VmsTable(name="district")
@IdClass(District.DistrictId.class)
public class District implements IEntity<District.DistrictId> {

    public static class DistrictId implements Serializable {
        public final Integer d_id;
        public final Integer d_w_id;

        public DistrictId(Integer d_id, Integer d_w_id) {
            this.d_id = d_id;
            this.d_w_id = d_w_id;
        }
    }

    @Id
    public int d_id;

    @Id
    @VmsForeignKey(table=Warehouse.class,column = "w_id")
    public int d_w_id;

    @Column
    public double d_tax;

    @Column
    public double d_ytd;

    @Column
    public int d_next_o_id;

    /*
    d_id ]] .. tinyint_type .. [[ not null,
	d_w_id smallint not null,
	d_name varchar(10),
	d_street_1 varchar(20),
	d_street_2 varchar(20),
	d_city varchar(20),
	d_state char(2),
	d_zip char(9),
	d_tax decimal(4,2),
	d_ytd decimal(12,2),
	d_next_o_id Integer,
	primary key (d_w_id, d_id)
     */

    public District(){}

    public District(int d_id, int d_w_id, double d_tax, double d_ytd, int d_next_o_id) {
        this.d_id = d_id;
        this.d_w_id = d_w_id;
        this.d_tax = d_tax;
        this.d_ytd = d_ytd;
        this.d_next_o_id = d_next_o_id;
    }
}
