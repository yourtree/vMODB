package dk.ku.di.dms.vms.tpcc.service;

import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.tpcc.repository.IItemRepository;

@Microservice("item")
public class ItemService {

    private final IItemRepository itemRepository;

    public ItemService(IItemRepository itemRepository){
        this.itemRepository = itemRepository;
    }

}
