package dk.ku.di.dms.vms.marketplace.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class ProductUpdatedEvent {

    public int sellerId;

    public int productId;

    public String version;

    public ProductUpdatedEvent(){}

    public ProductUpdatedEvent(int sellerId, int productId, String version) {
        this.sellerId = sellerId;
        this.productId = productId;
        this.version = version;
    }
}