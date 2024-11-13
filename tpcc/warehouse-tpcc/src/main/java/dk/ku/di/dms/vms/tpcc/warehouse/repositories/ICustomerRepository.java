package dk.ku.di.dms.vms.tpcc.warehouse.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;

@Repository
public interface ICustomerRepository extends IRepository<Customer.CustomerId, Customer> {

    @Query("select c_discount from customer where c_w_id = :d_w_id and c_d_id = :d_id and c_id = :c_id")
    float getDiscount(int c_w_id, int c_d_id, int c_id);

}