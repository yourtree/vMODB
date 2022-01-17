package dk.ku.di.dms.vms.tpcc.entity;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name="customer")
@IdClass(Customer.CustomerId.class)
public class Customer {

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
    public Long c_id;

    @Id
    public Integer c_d_id;

    @Id
    public Integer c_w_id;

    @Column
    public Float c_discount;

    @Column
    public String c_last;

    @Column
    public String c_credit;

    @Column
    public Float c_balance;

    @Column
    public Float c_ytd_payment;

    public Customer(){}

}
