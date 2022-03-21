package dk.ku.di.dms.vms.tpcc.repository;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Query;
import dk.ku.di.dms.vms.annotations.Repository;
import dk.ku.di.dms.vms.infra.IRepository;
import dk.ku.di.dms.vms.tpcc.entity.History;
import dk.ku.di.dms.vms.tpcc.entity.Item;

import java.util.List;
import java.util.Map;

@Repository
public interface IHistoryRepository extends IRepository<Integer, History> {

    @Inbound(values = "item-new-order-in")
    @Outbound("item-new-order-out")
    @Query("SELECT i.i_id, i.i_price FROM item s WHERE WHERE s_i_id IN (:itemIds)")
    Map<Integer,Float> getItemsById(List<Integer> itemIds);

}
