package dk.ku.di.dms.vms.marketplace.common.events;

import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.entities.CustomerCheckout;
import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.Date;
import java.util.List;

@Event
public class StockConfirmed {

    public Date timestamp;

    public CustomerCheckout customerCheckout;

    public List<CartItem> items;

    public String instanceId;

    public StockConfirmed(){}

    public StockConfirmed(Date timestamp, CustomerCheckout customerCheckout, List<CartItem> items, String instanceId) {
        this.timestamp = timestamp;
        this.customerCheckout = customerCheckout;
        this.items = items;
        this.instanceId = instanceId;
    }

}
