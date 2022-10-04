package dk.ku.di.dms.vms.playground.app;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;

@Repository
public interface RepositoryExample extends IRepository<Long, EntityExample> {

    @Query("SELECT i_id, i_price FROM item WHERE s_i_id IN :itemIds")
    Tuple<int[],float[]> getItemsById(int[] itemIds);

}