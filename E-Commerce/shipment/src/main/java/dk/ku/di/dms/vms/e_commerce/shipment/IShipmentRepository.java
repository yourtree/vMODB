package dk.ku.di.dms.vms.e_commerce.shipment;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface IShipmentRepository extends IRepository<Long, Shipment> { }
