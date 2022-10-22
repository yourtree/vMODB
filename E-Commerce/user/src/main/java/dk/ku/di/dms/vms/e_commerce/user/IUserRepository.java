package dk.ku.di.dms.vms.e_commerce.user;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IUserRepository extends IRepository<Long, User> {

    @Query("select * from user where user.id = :customerId")
    User findByCustomerId(long customerId);

}