package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name="orders")
@IdClass(Order.OrderId.class)
public class Order extends AbstractEntity<Order.OrderId> {

    public class OrderId implements Serializable {

        public Integer o_id;
        public Integer o_d_id;
        public Integer o_w_id;

        public OrderId(Integer o_id, Integer o_d_id, Integer o_w_id) {
            this.o_w_id = o_w_id;
            this.o_d_id = o_d_id;
            this.o_id = o_id;
        }
    }

    @Id
    public Integer o_id;

    @Id
    public Integer o_d_id;

    @Id
    public Integer o_w_id;

    @Column
    public Integer o_c_id;

//    @Column
//    public Long o_entry_d;
//
//    @Column
//    public Integer o_carrier_id;

    @Column
    public Integer o_ol_cnt;

//    @Column
//    public Integer o_all_local;

    public Order(){}

    public Order(Integer o_id, Integer o_d_id, Integer o_w_id, Integer o_c_id, Integer o_ol_cnt) {
        this.o_id = o_id;
        this.o_d_id = o_d_id;
        this.o_w_id = o_w_id;
        this.o_c_id = o_c_id;
        this.o_ol_cnt = o_ol_cnt;
        // super(new OrderId(o_id,o_d_id,o_w_id));
    }

}
