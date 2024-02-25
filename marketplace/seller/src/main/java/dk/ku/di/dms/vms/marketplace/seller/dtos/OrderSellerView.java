package dk.ku.di.dms.vms.marketplace.seller.dtos;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class OrderSellerView {

    // mark as not serializable
    public transient Set<OrderId> orders;

    public record OrderId(int customerId, int orderId) {}

    public int seller_id;

    public int count_orders;
    public int count_items;

    public float total_amount;
    public float total_freight;

    public float total_incentive;

    public float total_invoice;
    public float total_items;

    public OrderSellerView() {
        // this.orders = Set.of();
    }

    public OrderSellerView(int sellerId) {
        this.seller_id = sellerId;
        this.orders = ConcurrentHashMap.newKeySet();
    }

}