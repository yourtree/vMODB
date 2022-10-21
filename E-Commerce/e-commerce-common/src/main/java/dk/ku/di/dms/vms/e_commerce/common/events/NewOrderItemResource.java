package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.e_commerce.common.entity.Item;
import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.List;

@Event
public class NewOrderItemResource {

//    int[] itemsIds;
//    int[] quantity;
//    float[] prices;
    public List<Item> items;

    public NewOrderItemResource(List<Item> items) {
        this.items = items;
    }
}
