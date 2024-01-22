package dk.ku.di.dms.vms.marketplace.common.events;

import java.util.Date;

public class DeliveryNotification {

    public int customerId;

    public int orderId;

    public int packageId;

    public int sellerId;

    public int productId;

    public String productName;

    public String packageStatus;

    public Date deliveryDate;
    
    public DeliveryNotification(){}

    public DeliveryNotification(int customerId, int orderId, int packageId, int sellerId, int productId, String productName, String packageStatus, Date deliveryDate) {
        this.customerId = customerId;
        this.orderId = orderId;
        this.packageId = packageId;
        this.sellerId = sellerId;
        this.productId = productId;
        this.productName = productName;
        this.packageStatus = packageStatus;
        this.deliveryDate = deliveryDate;
    }

}
