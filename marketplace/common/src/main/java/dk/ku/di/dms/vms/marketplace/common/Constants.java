package dk.ku.di.dms.vms.marketplace.common;

public final class Constants {

    /**
     * PORTS
     */
    public static final int CART_HTTP_PORT = 8000;
    public static final int CART_VMS_PORT = 8080;

    public static final int PRODUCT_HTTP_PORT = 8001;
    public static final int PRODUCT_VMS_PORT = 8081;

    public static final int STOCK_HTTP_PORT = 8002;
    public static final int STOCK_VMS_PORT = 8082;

    public static final int ORDER_VMS_PORT = 8083;

    public static final int PAYMENT_VMS_PORT = 8084;

    public static final int SHIPMENT_VMS_PORT = 8085;

    public static final int CUSTOMER_VMS_PORT = 8086;

    public static final int SELLER_VMS_PORT = 8087;

    /**
     * EVENTS
     */
    public static final String CUSTOMER_CHECKOUT = "customer_checkout";

    public static final String PRODUCT_UPDATED = "product_updated";

    public static final String UPDATE_PRICE = "update_price";

    public static final String RESERVE_STOCK = "reserve_stock";

    public static final String STOCK_CONFIRMED = "stock_confirmed";

}
