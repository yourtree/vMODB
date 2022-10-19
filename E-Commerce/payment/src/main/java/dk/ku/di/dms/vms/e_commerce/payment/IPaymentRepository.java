package dk.ku.di.dms.vms.e_commerce.payment;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IPaymentRepository extends IRepository<Long, Payment> {
}
