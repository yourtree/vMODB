package dk.ku.di.dms.vms.micro_tpcc.repository;

import dk.ku.di.dms.vms.micro_tpcc.entity.Item;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface IItemRepository extends IRepository<Integer, Item> {

    @Inbound(values = "item-new-order-in")
    @Outbound("item-new-order-out")
    @Query("SELECT i.i_id, i.i_price FROM item s WHERE s_i_id IN (:itemIds)") // would be nice to verify prior to execution
    Map<Integer,Float> getItemsById(List<Integer> itemIds);

}
