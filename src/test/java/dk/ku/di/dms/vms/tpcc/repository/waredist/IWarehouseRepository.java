package dk.ku.di.dms.vms.tpcc.repository.waredist;

import dk.ku.di.dms.vms.annotations.Query;
import dk.ku.di.dms.vms.annotations.Repository;
import dk.ku.di.dms.vms.infra.IRepository;
import dk.ku.di.dms.vms.tpcc.entity.Warehouse;

@Repository
public interface IWarehouseRepository extends IRepository<Integer, Warehouse> {

    @Query("SELECT w.w_tax FROM warehouse w WHERE w.w_id = :w_id")
    Float getWarehouseTax(Integer w_id);

}
