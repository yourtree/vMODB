package dk.ku.di.dms.vms.micro_tpcc.repository.waredist;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.micro_tpcc.entity.Warehouse;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IWarehouseRepository extends IRepository<Integer, Warehouse> {

    @Query("SELECT w.w_tax FROM warehouse w WHERE w.w_id = :w_id")
    Float getWarehouseTax(Integer w_id);

}
