package dk.ku.di.dms.vms.micro_tpcc.workload;

import dk.ku.di.dms.vms.sdk.core.annotations.Microservice;
import dk.ku.di.dms.vms.sdk.core.annotations.Transactional;

@Microservice("new_order")
public class NewOrderTransaction {

    @Transactional
    public void run( final NewOrderTransactionInput input ){



        return;
    }

}
