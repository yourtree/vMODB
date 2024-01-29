package dk.ku.di.dms.vms.marketplace.seller;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Date;

@Entity
@VmsTable(name="order_entries")
public class OrderEntry implements IEntity<Integer> {

    public static class OrderEntryId implements Serializable {

        public int customer_id;
        public int order_id;
        public int package_id;

        public OrderEntryId(){}

        public OrderEntryId(int customer_id, int order_id, int package_id) {
            this.customer_id = customer_id;
            this.order_id = order_id;
            this.package_id = package_id;
        }

    }

    public OrderEntryId getId(){
        return new OrderEntryId( this.customer_id, this.order_id, this.package_id );
    }

    @Id
    public int customer_id;

    @Id
    public int order_id;

    @Id
    public int package_id;

    @Column
    public int seller_id;

    @Column
    public int product_id;

    @Column
    public String product_name;

    @Column
    public float freight_value;

    @Column
    public Date shipping_date;

    @Column
    public Date delivery_date;

    @Column
    public int quantity;


    public OrderEntry() { }

    public OrderEntry(int customer_id, int order_id, int package_id, int seller_id,
                      int product_id, String product_name, float freight_value, Date shipping_date,
                      Date delivery_date, int quantity  ) {
        this.customer_id = customer_id;
        this.order_id = order_id;
        this.package_id = package_id;
        this.seller_id = seller_id;
        this.product_id = product_id;
        this.product_name = product_name;
        this.freight_value = freight_value;
        this.shipping_date = shipping_date;
        this.delivery_date = delivery_date;
        this.quantity = quantity;
    }

    public int getSellerId() {
        return this.seller_id;
    }

    public Date getShippingDate(){
        return this.shipping_date;
    }

}
