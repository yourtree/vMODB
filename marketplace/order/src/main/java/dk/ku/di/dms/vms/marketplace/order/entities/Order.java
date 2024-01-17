package dk.ku.di.dms.vms.marketplace.order.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.Date;

@Entity
@VmsTable(name="orders")
public class Order implements IEntity<Integer> {

    @Id
    public int id;

    @Column
    public String invoice_number;

    @Column
    public int customer_id;

    @Column
    public OrderStatus status;

    @Column
    public Date purchase_date;

    @Column
    public Date payment_date;

    @Column
    public Date delivered_carrier_date;

    @Column
    public Date delivered_customer_date;

    @Column
    public Date estimated_delivery_date;

    @Column
    public int count_items;

    @Column
    public Date created_at;

    @Column
    public Date updated_at;

    @Column
    public float total_amount;

    @Column
    public float total_freight;

    @Column
    public float total_incentive;

    @Column
    public float total_invoice;

    @Column
    public float total_items;
    
    public Order(){}

    public Order(int id, String invoice_number, int customer_id, OrderStatus status, Date purchase_date,
                 Date payment_date, Date delivered_carrier_date, Date delivered_customer_date, Date estimated_delivery_date,
                 int count_items, Date created_at, Date updated_at, float total_amount, float total_freight,
                 float total_incentive, float total_invoice, float total_items) {
        this.id = id;
        this.invoice_number = invoice_number;
        this.customer_id = customer_id;
        this.status = status;
        this.purchase_date = purchase_date;
        this.payment_date = payment_date;
        this.delivered_carrier_date = delivered_carrier_date;
        this.delivered_customer_date = delivered_customer_date;
        this.estimated_delivery_date = estimated_delivery_date;
        this.count_items = count_items;
        this.created_at = created_at;
        this.updated_at = updated_at;
        this.total_amount = total_amount;
        this.total_freight = total_freight;
        this.total_incentive = total_incentive;
        this.total_invoice = total_invoice;
        this.total_items = total_items;
    }

}
