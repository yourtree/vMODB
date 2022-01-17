package dk.ku.di.dms.vms.eShopOnContainers.repository;

import dk.ku.di.dms.vms.annotations.Repository;
import dk.ku.di.dms.vms.infra.IRepository;
import dk.ku.di.dms.vms.eShopOnContainers.entity.Product;

@Repository
public interface IProductRepository extends IRepository<Long, Product> {

}
