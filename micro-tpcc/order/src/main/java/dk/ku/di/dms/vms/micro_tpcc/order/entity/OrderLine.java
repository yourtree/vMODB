package dk.ku.di.dms.vms.micro_tpcc.order.entity;

import dk.ku.di.dms.vms.micro_tpcc.entity.Stock;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsIndex;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

@Entity
@VmsTable(name="order_line",
        indexes = {@VmsIndex(name = "fkey_order_line_2", columnList = "ol_supply_w_id,ol_i_id")
        })
@IdClass(OrderLine.OrderLineId.class)
public class OrderLine implements IEntity<OrderLine.OrderLineId> {

    public static class OrderLineId implements Serializable {
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
    @VmsForeignKey(table=Order.class,column = "o_id")
    public int ol_o_id;

    @Id
    @VmsForeignKey(table=Order.class,column = "o_d_id")
    public int ol_d_id;

    @Id
    @VmsForeignKey(table=Order.class,column = "o_w_id")
    public int ol_w_id;

    @Id
    public int ol_number;

    @VmsForeignKey(table= Stock.class,column = "s_i_id")
    public int ol_i_id;

    @VmsForeignKey(table=Stock.class,column = "s_w_id")
    public int ol_supply_w_id;

    @Column
    public Date ol_delivery_d;

    @Column
    public int ol_quantity;

    @Column
    public float ol_amount;

    @Column
    public String ol_dist_info;

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

    public OrderLine(int ol_o_id, int ol_d_id, int ol_w_id, int ol_number, int ol_i_id, int ol_supply_w_id, Date ol_delivery_d, int ol_quantity, float ol_amount, String ol_dist_info) {
        this.ol_o_id = ol_o_id;
        this.ol_d_id = ol_d_id;
        this.ol_w_id = ol_w_id;
        this.ol_number = ol_number;
        this.ol_i_id = ol_i_id;
        this.ol_supply_w_id = ol_supply_w_id;
        this.ol_delivery_d = ol_delivery_d;
        this.ol_quantity = ol_quantity;
        this.ol_amount = ol_amount;
        this.ol_dist_info = ol_dist_info;
    }
}
