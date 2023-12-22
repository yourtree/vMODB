package dk.ku.di.dms.vms.marketplace.product;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IProductRepository extends IRepository<Product.ProductId, Product> {
}
