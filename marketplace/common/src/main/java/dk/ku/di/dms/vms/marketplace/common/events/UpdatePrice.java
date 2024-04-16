package dk.ku.di.dms.vms.marketplace.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class UpdatePrice {

    public int sellerId;

    public int productId;

    public float price;

    public String instanceId;

    public UpdatePrice(){}

    public UpdatePrice(int sellerId,
                       int productId,
                       float price,
                       String instanceId) {
        this.sellerId = sellerId;
        this.productId = productId;
        this.price = price;
        this.instanceId = instanceId;
    }

    public ProductId getId(){
        return new ProductId(this.sellerId, this.productId);
    }

    public record ProductId( int sellerId, int productId){}

}
