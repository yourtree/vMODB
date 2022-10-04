package dk.ku.di.dms.vms.micro_tpcc.customer.repository;

import dk.ku.di.dms.vms.micro_tpcc.customer.entity.History;
import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;

@Repository
public interface IHistoryRepository extends IRepository<Integer, History> {

    @Query("SELECT i_id, i_price FROM item WHERE s_i_id IN :itemIds")
    Tuple<int[],float[]> getItemsById(int[] itemIds);

}
