package dk.ku.di.dms.vms.marketplace.cart.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsIndex;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;

@VmsTable(name="cart_items")
@IdClass(CartItem.CartItemId.class)
public final class CartItem implements IEntity<CartItem.CartItemId> {

    public static class CartItemId implements Serializable {
        public int seller_id;
        public int product_id;
        public int customer_id;

        public CartItemId(){}

        public CartItemId(int customer_id, int product_id, int seller_id) {
            this.customer_id = customer_id;
            this.product_id = product_id;
            this.seller_id = seller_id;
        }
    }

    @Id
    @VmsForeignKey(table = ProductReplica.class, column = "seller_id")
    public int seller_id;

    @Id
    @VmsForeignKey(table = ProductReplica.class, column = "product_id")
    public int product_id;

    // another way to create index on customer:
    // @VmsForeignKey(table = Cart.class, column = "id")
    @Id
    @VmsIndex(name = "customerIdx")
    public int customer_id;

    @Column
    public String product_name;

    @Column
    public float unit_price;

    @Column
    public float freight_value;

    @Column
    public int quantity;

    @Column
    public float voucher;

    @Column
    public String version;

    public CartItem() {}

    public CartItem(int seller_id, int product_id, int customer_id, String product_name, float unit_price, float freight_value, int quantity, float voucher, String version) {
        this.customer_id = customer_id;
        this.freight_value = freight_value;
        this.product_id = product_id;
        this.product_name = product_name;
        this.quantity = quantity;
        this.seller_id = seller_id;
        this.unit_price = unit_price;
        this.version = version;
        this.voucher = voucher;
    }

    @Override
    public String toString() {
        return "{"
                + " \"customer_id\":\"" + customer_id + "\""
                + ",\"seller_id\":\"" + seller_id + "\""
                + ",\"product_id\":\"" + product_id + "\""
                + ",\"product_name\":\"" + product_name + "\""
                + ",\"unit_price\":\"" + unit_price + "\""
                + ",\"freight_value\":\"" + freight_value + "\""
                + ",\"quantity\":\"" + quantity + "\""
                + ",\"voucher\":\"" + voucher + "\""
                + ",\"version\":\"" + version + "\""
                + "}";
    }
}
