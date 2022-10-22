package dk.ku.di.dms.vms.e_commerce.user;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface ICardRepository extends IRepository<Long, Card> { }
