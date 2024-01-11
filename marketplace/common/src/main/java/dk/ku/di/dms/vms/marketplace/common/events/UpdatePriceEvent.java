package dk.ku.di.dms.vms.marketplace.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class UpdatePriceEvent {

    public int sellerId;

    public int productId;

    public float price;

    public String instanceId;

    public UpdatePriceEvent(){}

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
