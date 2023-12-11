package dk.ku.di.dms.vms.playground.scenario2;

import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.dms.vms.playground.app.OutEventExample2;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("example2")
public class MicroserviceExample2 {

    @Inbound(values = {"out2"})
    @Outbound("out3")
    @Transactional(type=RW)
    public OutEventExample3 methodExample2(OutEventExample2 out) {
        System.out.println("I am methodExample2!");
        return out != null ? new OutEventExample3(out.id) : null;
    }

}
