package dk.ku.di.dms.vms.micro_tpcc.repository.waredist;

import dk.ku.di.dms.vms.micro_tpcc.dto.DistrictInfoDTO;
import dk.ku.di.dms.vms.micro_tpcc.entity.District;
import dk.ku.di.dms.vms.modb.common.interfaces.IRepository;
import dk.ku.di.dms.vms.sdk.core.annotations.Query;
import dk.ku.di.dms.vms.sdk.core.annotations.Repository;

@Repository
public interface IDistrictRepository extends IRepository<District.DistrictId, District> {

    @Query("select d_next_o_id, d_tax from district where d_w_id = :d_w_id and d_id = :d_id")
    DistrictInfoDTO getNextOidAndTax(Integer d_w_id, Integer d_id);

}
