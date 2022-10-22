package dk.ku.di.dms.vms.e_commerce.common.events;

import dk.ku.di.dms.vms.e_commerce.common.entity.Item;

import java.util.List;

public class NewOrderItemResponse {

    public List<Item> items;
    public NewOrderItemResponse(List<Item> items) {
        this.items = items;
    }
}
