package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.annotations.VmsTable;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@VmsTable(name="order_line")
@IdClass(OrderLine.OrderLineId.class)
public class OrderLine extends AbstractEntity<OrderLine.OrderLineId> {

    public class OrderLineId implements Serializable {
        public Integer ol_o_id;
        public Integer ol_d_id;
        public Integer ol_w_id;
        public Integer ol_number;

        public OrderLineId(Integer ol_o_id, Integer ol_d_id, Integer ol_w_id, Integer ol_number) {
            this.ol_o_id = ol_o_id;
            this.ol_d_id = ol_d_id;
            this.ol_w_id = ol_w_id;
            this.ol_number = ol_number;
        }
    }

    @Id
    public Integer ol_o_id;

    @Id
    public Integer ol_d_id;

    @Id
    public Integer ol_w_id;

    @Id
    public Integer ol_number;

    @Column
    public Integer ol_i_id;

    @Column
    public Integer ol_supply_w_id;

    @Column
    public Integer ol_quantity;

    @Column
    public Float ol_amount;

    @Column
    public Float ol_dist_info;

    /*
    ol_o_id Integer not null,
	ol_d_id ]] .. tinyint_type .. [[ not null,
	ol_w_id smallint not null,
	ol_number ]] .. tinyint_type .. [[ not null,
	ol_i_id Integer,
	ol_supply_w_id smallint,
	ol_delivery_d ]] .. datetime_type .. [[,
	ol_quantity ]] .. tinyint_type .. [[,
	ol_amount decimal(6,2),
	ol_dist_info char(24),
	PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number)
     */

    public OrderLine(){}

}
