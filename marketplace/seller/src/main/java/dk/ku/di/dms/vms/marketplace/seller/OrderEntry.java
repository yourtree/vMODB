package dk.ku.di.dms.vms.marketplace.seller;

import dk.ku.di.dms.vms.marketplace.common.enums.OrderStatus;
import dk.ku.di.dms.vms.marketplace.common.enums.PackageStatus;
import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Date;

@VmsTable(name="order_entries")
public final class OrderEntry implements IEntity<OrderEntry.OrderEntryId> {

    public static class OrderEntryId implements Serializable {

        public int customer_id;
        public int order_id;
        public int product_id;

        public OrderEntryId() {
        }

        public OrderEntryId(int customer_id, int order_id, int product_id) {
            this.customer_id = customer_id;
            this.order_id = order_id;
            this.product_id = product_id;
        }

    }

    @Id
    public int customer_id;

    @Id
    public int order_id;

    @Id
    public int product_id;

    @Column
    @VmsForeignKey(table = Seller.class, column = "id")
    public int seller_id;

    @Column
    public int package_id;

    @Column
    public String product_name;

    @Column
    public String product_category = "";

    @Column
    public float unit_price;

    @Column
    public int quantity;

    @Column
    public float total_items;

    @Column
    public float total_amount;

    @Column
    public float total_invoice;

    @Column
    public float total_incentive;

    @Column
    public float freight_value;

    @Column
    public Date shipment_date;

    @Column
    public Date delivery_date;

    @Column
    public OrderStatus order_status;

    @Column
    public PackageStatus delivery_status;

    public OrderEntry() { }

    public OrderEntry(int customer_id, int order_id, int package_id, int seller_id, int product_id, String product_name, String product_category, float unit_price, int quantity, float total_items, float total_amount, float total_invoice, float total_incentive, float freight_value, Date shipment_date, Date delivery_date, OrderStatus order_status, PackageStatus delivery_status) {
        this.customer_id = customer_id;
        this.order_id = order_id;
        this.package_id = package_id;
        this.seller_id = seller_id;
        this.product_id = product_id;
        this.product_name = product_name;
        this.product_category = product_category;
        this.unit_price = unit_price;
        this.quantity = quantity;
        this.total_items = total_items;
        this.total_amount = total_amount;
        this.total_invoice = total_invoice;
        this.total_incentive = total_incentive;
        this.freight_value = freight_value;
        this.shipment_date = shipment_date;
        this.delivery_date = delivery_date;
        this.order_status = order_status;
        this.delivery_status = delivery_status;
    }

    public OrderEntryId getId(){
        return new OrderEntryId( this.customer_id, this.order_id, this.package_id );
    }

}
