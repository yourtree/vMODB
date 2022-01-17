package dk.ku.di.dms.vms.tpcc.entity;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name="district")
@IdClass(District.DistrictId.class)
public class District {

    public class DistrictId implements Serializable {
        public Integer d_id;
        public Integer d_w_id;

        public DistrictId(Integer d_id, Integer d_w_id) {
            this.d_id = d_id;
            this.d_w_id = d_w_id;
        }
    }

    @Id
    public Integer d_id;

    @Id
    public Integer d_w_id;

    @Column
    public Float d_tax;

    @Column
    public Float d_ytd;

    @Column
    public Integer d_next_o_id;

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

}
