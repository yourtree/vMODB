package dk.ku.di.dms.vms.marketplace.order.repositories;

import dk.ku.di.dms.vms.marketplace.order.entities.OrderItem;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IOrderItemRepository extends IRepository<OrderItem.OrderItemId, OrderItem> { }
