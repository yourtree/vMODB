package dk.ku.di.dms.vms.tpcc.repository.waredist;

import dk.ku.di.dms.vms.annotations.Query;
import dk.ku.di.dms.vms.annotations.Repository;
import dk.ku.di.dms.vms.infra.IRepository;
import dk.ku.di.dms.vms.tpcc.entity.District;
import dk.ku.di.dms.vms.utils.Pair;

@Repository
public interface IDistrictRepository extends IRepository<District.DistrictId, District> {

    @Query("select d_next_o_id, d_tax from district where d_w_id = :d_w_id and d_id = :d_id")
    Pair<Integer, Float> getNextOidAndTax(Integer d_w_id, Integer d_id);

}
