package dk.ku.di.dms.vms.tpcc.remote;

import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.infra.IRemoteService;
import dk.ku.di.dms.vms.tpcc.events.WareDistNewOrderOut;

// @Microservice("warehouse")
public interface IRemoteWarehouse extends IRemoteService {

    WareDistNewOrderOut getWarehouseTax(final Integer w_id);

}
