package dk.ku.di.dms.vms.tpcc.workload;

import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.tpcc.workload.NewOrderTransactionInput;

@Microservice("new_order")
public class NewOrderTransaction {

    @Transactional
    public void run( final NewOrderTransactionInput input ){



        return;
    }

}
