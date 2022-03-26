package dk.ku.di.dms.vms.eshop.repository;

import dk.ku.di.dms.vms.eshop.entity.Product;
import dk.ku.di.dms.vms.modb.common.IRepository;
import dk.ku.di.dms.vms.sdk.core.annotations.Repository;

@Repository
public interface IProductRepository extends IRepository<Long, Product> {

}
