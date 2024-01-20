package dk.ku.di.dms.vms.marketplace.payment.repositories;

import dk.ku.di.dms.vms.marketplace.payment.entities.OrderPayment;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IOrderPaymentRepository extends IRepository<OrderPayment.OrderPaymentId, OrderPayment> { }
