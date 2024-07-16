package dk.ku.di.dms.vms.marketplace.common.events;

import dk.ku.di.dms.vms.marketplace.common.inputs.PriceUpdate;
import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class PriceUpdated {

    public int sellerId;

    public int productId;

    public float price;

    public String instanceId;

    public PriceUpdated(){}

    public PriceUpdated(int sellerId,
                       int productId,
                       float price,
                       String instanceId) {
        this.sellerId = sellerId;
        this.productId = productId;
        this.price = price;
        this.instanceId = instanceId;
    }

    public PriceUpdate.ProductId getId(){
        return new PriceUpdate.ProductId(this.sellerId, this.productId);
    }

    public record ProductId( int sellerId, int productId){}

}
