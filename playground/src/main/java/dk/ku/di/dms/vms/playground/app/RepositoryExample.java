package dk.ku.di.dms.vms.playground.app;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;

@Repository
public interface RepositoryExample extends IRepository<Long, EntityExample> {

}