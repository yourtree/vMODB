package dk.ku.di.dms.vms.micro_tpcc.warehouse.repository;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.micro_tpcc.common.entity.Warehouse;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IWarehouseRepository extends IRepository<Integer, Warehouse> {

    @Query("SELECT w_tax FROM warehouse WHERE w_id = :w_id")
    Float getWarehouseTax(Integer w_id);

}
