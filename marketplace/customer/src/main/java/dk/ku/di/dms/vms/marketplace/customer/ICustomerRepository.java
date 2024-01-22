package dk.ku.di.dms.vms.marketplace.customer;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface ICustomerRepository extends IRepository<Integer, Customer> { }
