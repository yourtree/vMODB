package dk.ku.di.dms.vms.playground.micro_tpcc;

import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

@Microservice("new_order")
public class NewOrderTransaction {

    @Transactional
    public void run( final NewOrderTransactionInput input ){



        return;
    }

}
