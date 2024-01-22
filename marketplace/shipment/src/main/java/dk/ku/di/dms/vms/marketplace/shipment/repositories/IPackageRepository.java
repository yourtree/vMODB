package dk.ku.di.dms.vms.marketplace.shipment.repositories;

import dk.ku.di.dms.vms.marketplace.shipment.entities.Package;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

public interface IPackageRepository extends IRepository<Package.PackageId, Package> {
}
