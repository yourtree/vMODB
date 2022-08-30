package dk.ku.di.dms.vms.micro_tpcc.repository;

import dk.ku.di.dms.vms.modb.common.interfaces.application.IRepository;
import dk.ku.di.dms.vms.sdk.core.annotations.Inbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Outbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Query;
import dk.ku.di.dms.vms.sdk.core.annotations.Repository;
import dk.ku.di.dms.vms.micro_tpcc.entity.History;

import java.util.List;
import java.util.Map;

@Repository
public interface IHistoryRepository extends IRepository<Integer, History> {

    @Inbound(values = "item-new-order-in")
    @Outbound("item-new-order-out")
    @Query("SELECT i.i_id, i.i_price FROM item s WHERE s_i_id IN (:itemIds)")
    Map<Integer,Float> getItemsById(List<Integer> itemIds);

}
