package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.e_commerce.common.entity.Item;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IItemRepository extends IRepository<Long, Item> { }
