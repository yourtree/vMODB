package dk.ku.di.dms.vms.micro_tpcc.service;

import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.micro_tpcc.repository.IItemRepository;

@Microservice("item")
public class ItemService {

    private final IItemRepository itemRepository;

    public ItemService(IItemRepository itemRepository){
        this.itemRepository = itemRepository;
    }

}
