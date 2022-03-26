package dk.ku.di.dms.vms.micro_tpcc.entity;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.sdk.core.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.sdk.core.annotations.VmsIndex;
import dk.ku.di.dms.vms.sdk.core.annotations.VmsTable;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

@Entity
@VmsTable(name="orders",
        indexes = {@VmsIndex(name = "idx_orders", columnList = "o_w_id,o_d_id,o_c_id,o_id", unique = true)
})
@IdClass(Order.OrderId.class)
public class Order implements IEntity<Order.OrderId> {

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
    public int o_id;

    @Id
    @VmsForeignKey(table=Customer.class,column = "c_d_id")
    public int o_d_id;

    @Id
    @VmsForeignKey(table=Customer.class,column = "c_w_id")
    public int o_w_id;

    @VmsForeignKey(table=Customer.class,column = "c_id")
    public int o_c_id;

    @Column
    public Date o_entry_d;

    @Column
    public int o_carrier_id;

    @Column
    public int o_ol_cnt;

    @Column
    public int o_all_local;

    public Order(){}

    public Order(int o_id, int o_d_id, int o_w_id, int o_c_id, Date o_entry_d, int o_carrier_id, int o_ol_cnt, int o_all_local) {
        this.o_id = o_id;
        this.o_d_id = o_d_id;
        this.o_w_id = o_w_id;
        this.o_c_id = o_c_id;
        this.o_entry_d = o_entry_d;
        this.o_carrier_id = o_carrier_id;
        this.o_ol_cnt = o_ol_cnt;
        this.o_all_local = o_all_local;
    }
}
