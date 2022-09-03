package dk.ku.di.dms.vms.micro_tpcc.repository;

import dk.ku.di.dms.vms.modb.common.interfaces.IRepository;
import dk.ku.di.dms.vms.sdk.core.annotations.Repository;
import dk.ku.di.dms.vms.micro_tpcc.entity.Customer;

@Repository
public interface ICustomerRepository extends IRepository<Customer.CustomerId, Customer> {

}
