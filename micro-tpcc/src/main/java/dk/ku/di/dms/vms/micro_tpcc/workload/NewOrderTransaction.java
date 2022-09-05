package dk.ku.di.dms.vms.micro_tpcc.workload;

import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

@Microservice("new_order")
public class NewOrderTransaction {

    @Transactional
    public void run( final NewOrderTransactionInput input ){



        return;
    }

}
