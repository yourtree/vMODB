package dk.ku.di.dms.vms.tpcc.entity;

import dk.ku.di.dms.vms.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.annotations.VmsIndex;
import dk.ku.di.dms.vms.annotations.VmsTable;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@VmsTable(name="customer",
        indexes = {@VmsIndex(name = "idx_customer", columnList = "c_w_id,c_d_id,c_last,c_first", unique = false) // TODO check if this is unique
})
@IdClass(Customer.CustomerId.class)
public class Customer extends AbstractEntity<Customer.CustomerId> {

//    @Override
//    public CustomerId primaryKey() {
//        return new CustomerId(c_id,c_d_id,c_w_id);
//    }

    public class CustomerId implements Serializable {
        public Long c_id;
        public Integer c_d_id;
        public Integer c_w_id;

        public CustomerId(){}

        public CustomerId(Long c_id, Integer c_d_id, Integer c_w_id) {
            this.c_id = c_id;
            this.c_d_id = c_d_id;
            this.c_w_id = c_w_id;
        }
    }

    @Id
    public int c_id;

    @Id
    @VmsForeignKey(table=District.class,column = "d_id")
    public int c_d_id;

    @Id
    @VmsForeignKey(table=District.class,column = "d_w_id")
    public int c_w_id;

    @Column
    public float c_discount;

    @Column
    public String c_first;

    @Column
    public String c_last;

    @Column
    public String c_credit;

    @Column
    public float c_balance;

    @Column
    public float c_ytd_payment;

    public Customer(){}

    public Customer(int c_id, int c_d_id, int c_w_id, float c_discount, String c_first, String c_last, String c_credit, float c_balance, float c_ytd_payment) {
        this.c_id = c_id;
        this.c_d_id = c_d_id;
        this.c_w_id = c_w_id;
        this.c_discount = c_discount;
        this.c_first = c_first;
        this.c_last = c_last;
        this.c_credit = c_credit;
        this.c_balance = c_balance;
        this.c_ytd_payment = c_ytd_payment;
    }
}
