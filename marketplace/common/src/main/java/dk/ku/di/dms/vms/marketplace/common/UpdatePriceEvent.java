package dk.ku.di.dms.vms.marketplace.common;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class UpdatePriceEvent {

    public final int sellerId;

    public final int productId;

    public final float price;

    public final String instanceId;

    public UpdatePriceEvent(int sellerId,
                       int productId,
                       float price,
                       String instanceId) {
        this.sellerId = sellerId;
        this.productId = productId;
        this.price = price;
        this.instanceId = instanceId;
    }

}
