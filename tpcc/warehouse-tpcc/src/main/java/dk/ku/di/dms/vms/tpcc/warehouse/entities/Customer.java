package dk.ku.di.dms.vms.tpcc.warehouse.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;
import java.util.Date;

@Entity
@VmsTable(name="customer")
@IdClass(Customer.CustomerId.class)
public class Customer implements IEntity<Customer.CustomerId> {

    public static class CustomerId implements Serializable {
        public long c_id;
        public int c_d_id;
        public int c_w_id;

        public CustomerId(){}

        public CustomerId(long c_id, int c_d_id, int c_w_id) {
            this.c_id = c_id;
            this.c_d_id = c_d_id;
            this.c_w_id = c_w_id;
        }
    }

    @Id
    public int c_id;

    @Id
    @VmsForeignKey(table=District.class, column = "d_id")
    public int c_d_id;

    @Id
    @VmsForeignKey(table=District.class, column = "d_w_id")
    public int c_w_id;

    @Column
    public String c_first;

    @Column
    public String c_last;

    @Column
    public Date c_since;

    @Column
    public String c_credit;

    @Column
    public int c_credit_lim;

    @Column
    public float c_discount;

    @Column
    public float c_balance;

    @Column
    public float c_ytd_payment;

    public Customer(){}

    public Customer(int c_id, int c_d_id, int c_w_id,
                    String c_first, String c_middle, String c_last,
                    String  C_STREET_1, String C_STREET_2,
                    String C_CITY, String C_STATE, String C_ZIP, String C_PHONE,
                    Date c_since, String c_credit, int c_credit_lim, float c_discount, float c_balance, int c_ytd_payment, int C_PAYMENT_CNT, int C_DELIVERY_CNT, String C_DATA) {
        this.c_id = c_id;
        this.c_d_id = c_d_id;
        this.c_w_id = c_w_id;
        this.c_discount = c_discount;
        this.c_first = c_first;
        this.c_last = c_last;
        this.c_since = c_since;
        this.c_credit = c_credit;
        this.c_balance = c_balance;
        this.c_ytd_payment = c_ytd_payment;
    }

    @Override
    public String toString() {
        return "{"
                + "\"c_id\":\"" + c_id + "\""
                + ",\"c_w_id\":\"" + c_w_id + "\""
                + ",\"c_d_id\":\"" + c_d_id + "\""
                + ",\"c_first\":\"" + c_first + "\""
                + ",\"c_last\":\"" + c_last + "\""
                + ",\"c_since\":" + c_since.getTime()
                + ",\"c_credit\":\"" + c_credit + "\""
                + ",\"c_credit_lim\":\"" + c_credit_lim + "\""
                + ",\"c_discount\":\"" + c_discount + "\""
                + ",\"c_balance\":\"" + c_balance + "\""
                + ",\"c_ytd_payment\":\"" + c_ytd_payment + "\""
                + "}";
    }

}