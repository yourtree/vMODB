package dk.ku.di.dms.vms.marketplace.order.entities;

public enum OrderStatus {

    CREATED,
    PROCESSING,
    APPROVED,
    CANCELED,
    UNAVAILABLE,
    INVOICED,
    READY_FOR_SHIPMENT,
    IN_TRANSIT,
    DELIVERED,

    // created for the benchmark
    PAYMENT_FAILED,
    PAYMENT_PROCESSED
}
