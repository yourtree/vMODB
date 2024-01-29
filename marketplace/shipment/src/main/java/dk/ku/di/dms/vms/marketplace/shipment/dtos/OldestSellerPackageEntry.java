package dk.ku.di.dms.vms.marketplace.shipment.dtos;

import java.util.Date;

public class OldestSellerPackageEntry {

    int seller_id;
    int customer_id;
    int order_id;
    Date shipping_date;

    public OldestSellerPackageEntry() {
    }

    public int getSellerId() {
        return seller_id;
    }

    public int getCustomerId() {
        return customer_id;
    }

    public int getOrderId() {
        return order_id;
    }

    public Date getShippingDate() {
        return shipping_date;
    }
}
