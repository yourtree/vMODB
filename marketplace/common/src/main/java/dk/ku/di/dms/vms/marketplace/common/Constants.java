package dk.ku.di.dms.vms.marketplace.common;

public class Constants {

    /**
     * PORTS
     */
    public static final int CART_PORT = 8080;

    public static final int PRODUCT_PORT = 8081;

    public static final int STOCK_PORT = 8082;

    public static final int ORDER_PORT = 8083;

    public static final int PAYMENT_PORT = 8084;

    public static final int SHIPMENT_PORT = 8085;

    public static final int CUSTOMER_PORT = 8086;

    public static final int SELLER_PORT = 8087;

    /**
     * EVENTS
     */
    public static final String PRODUCT_UPDATED = "product_updated";

    public static final String RESERVE_STOCK = "reserve_stock";

    public static final String STOCK_CONFIRMED = "stock_confirmed";

}
