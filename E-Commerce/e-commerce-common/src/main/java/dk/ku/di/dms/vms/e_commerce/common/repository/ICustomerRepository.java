package dk.ku.di.dms.vms.e_commerce.common.repository;

import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface ICustomerRepository extends IRepository<Long, Customer> { }
