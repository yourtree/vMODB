package dk.ku.di.dms.vms.marketplace.common.entities;

public final class CartItem {

    public int SellerId;

    public int ProductId;

    public String ProductName;

    public float UnitPrice;

    public float FreightValue;

    public int Quantity;

    public float Voucher;

    public String Version;

    public CartItem() { }

    public CartItem(int sellerId, int productId, String productName, float unitPrice, float freightValue, int quantity, float voucher, String version) {
        SellerId = sellerId;
        ProductId = productId;
        ProductName = productName;
        UnitPrice = unitPrice;
        FreightValue = freightValue;
        Quantity = quantity;
        Voucher = voucher;
        Version = version;
    }
}
