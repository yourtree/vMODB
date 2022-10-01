package dk.ku.di.dms.vms.micro_tpcc.repository;

import dk.ku.di.dms.vms.micro_tpcc.entity.Item;
import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;

@Repository
public interface IItemRepository extends IRepository<Integer, Item> {

    @Query("SELECT i_id, i_price FROM item WHERE i_id IN (:itemIds)")
    Tuple<int[], float[]> getItemsById(int[] itemIds);

}
