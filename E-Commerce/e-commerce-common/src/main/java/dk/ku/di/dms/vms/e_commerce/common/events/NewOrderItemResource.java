package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public class NewOrderItemResource {

    int[] itemsIds;
    int[] quantity;
    float[] prices;
    // List<Item> items;

}
