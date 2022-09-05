package dk.ku.di.dms.vms.eshop.repository;

import dk.ku.di.dms.vms.eshop.entity.Product;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;

@Repository
public interface IProductRepository extends IRepository<Long, Product> {

}
