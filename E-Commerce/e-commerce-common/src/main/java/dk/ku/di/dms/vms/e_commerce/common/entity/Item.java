package dk.ku.di.dms.vms.e_commerce.common.entity;

/**
 * Clean object without cart information
 * In the future can have warehouse ID to show skewed and contentious workloads' difference
 */
public class Item {

    public long productId;

    public int quantity;

    public float unitPrice;

    public Item() {}

    public Item(long productId, int quantity, float unitPrice) {
        this.productId = productId;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
    }
}
