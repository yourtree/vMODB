package dk.ku.di.dms.vms.marketplace.payment.repositories;

import dk.ku.di.dms.vms.marketplace.payment.entities.OrderPaymentCard;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IOrderPaymentCardRepository extends IRepository<OrderPaymentCard.OrderPaymentCardId, OrderPaymentCard> { }
