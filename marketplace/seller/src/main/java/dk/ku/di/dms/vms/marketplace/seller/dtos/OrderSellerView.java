package dk.ku.di.dms.vms.marketplace.seller.dtos;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class OrderSellerView {

    // mark as not serializable
    public transient Set<OrderId> orders;

    public record OrderId(int customerId, int orderId) {}

    public int seller_id;

    public int count_orders;
    public int count_items;

    public double total_amount;
    public double total_freight;

    public double total_incentive;

    public double total_invoice;
    public double total_items;

    public OrderSellerView() { }

    public OrderSellerView(int sellerId) {
        this.seller_id = sellerId;
        this.orders = ConcurrentHashMap.newKeySet();
    }

    @Override
    public String toString() {
        return "{"
                + "\"count_items\":\"" + count_items + "\""
                + ",\"seller_id\":\"" + seller_id + "\""
                + ",\"count_orders\":\"" + count_orders + "\""
                + ",\"total_amount\":\"" + total_amount + "\""
                + ",\"total_freight\":\"" + total_freight + "\""
                + ",\"total_incentive\":\"" + total_incentive + "\""
                + ",\"total_invoice\":\"" + total_invoice + "\""
                + ",\"total_items\":\"" + total_items + "\""
                + "}";
    }

}