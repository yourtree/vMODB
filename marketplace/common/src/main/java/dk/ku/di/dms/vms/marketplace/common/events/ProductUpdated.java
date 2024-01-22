package dk.ku.di.dms.vms.marketplace.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class ProductUpdated {

    public int sellerId;

    public int productId;

    public String version;

    public ProductUpdated(){}

    public ProductUpdated(int sellerId, int productId, String version) {
        this.sellerId = sellerId;
        this.productId = productId;
        this.version = version;
    }
}