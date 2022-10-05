package dk.ku.di.dms.vms.micro_tpcc.warehouse.repository;

import dk.ku.di.dms.vms.micro_tpcc.warehouse.dto.DistrictInfoDTO;
import dk.ku.di.dms.vms.micro_tpcc.common.entity.District;
import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IDistrictRepository extends IRepository<District.DistrictId, District> {

    @Query("select d_next_o_id, d_tax from district where d_w_id = :d_w_id and d_id = :d_id")
    DistrictInfoDTO getNextOidAndTax(Integer d_w_id, Integer d_id);

}
