package dk.ku.di.dms.vms.micro_tpcc.customer.entity;

import dk.ku.di.dms.vms.micro_tpcc.entity.Customer;
import dk.ku.di.dms.vms.micro_tpcc.entity.District;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsIndex;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;

import javax.persistence.*;
import java.util.Date;

@Entity
@VmsTable(name="history",
        indexes = {@VmsIndex(name = "fkey_history_1", columnList = "h_c_w_id,h_c_d_id,h_c_id"),
                @VmsIndex(name = "fkey_history_2", columnList = "h_w_id,h_d_id")
        })
public class History implements IEntity<Integer> {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    public int id;

    @VmsForeignKey(table= Customer.class,column = "c_id")
    public int h_c_id;

    @VmsForeignKey(table=Customer.class,column = "c_w_id")
    public int h_c_w_id;

    @VmsForeignKey(table=Customer.class,column = "c_d_id")
    public int h_c_d_id;

    @VmsForeignKey(table= District.class,column = "d_w_id")
    public int h_w_id;

    @VmsForeignKey(table=District.class,column = "d_id")
    public int h_d_id;

    @Column
    public Date h_date;

    @Column
    public float h_amount;

    @Column
    public String h_data;

    public History(){}

    public History(int id, int h_c_id, int h_c_w_id, int h_c_d_id, int h_w_id, int h_d_id, Date h_date, float h_amount, String h_data) {
        this.id = id;
        this.h_c_id = h_c_id;
        this.h_c_w_id = h_c_w_id;
        this.h_c_d_id = h_c_d_id;
        this.h_w_id = h_w_id;
        this.h_d_id = h_d_id;
        this.h_date = h_date;
        this.h_amount = h_amount;
        this.h_data = h_data;
    }
}
