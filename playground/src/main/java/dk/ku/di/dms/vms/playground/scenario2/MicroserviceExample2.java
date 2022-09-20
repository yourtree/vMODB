package dk.ku.di.dms.vms.playground.scenario2;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.playground.app.OutEventExample;
import dk.ku.di.dms.vms.playground.app.OutEventExample2;

@Microservice("example2")
public class MicroserviceExample2 {

    @Inbound(values = {"out"})
    @Outbound("out3")
    @Transactional
    @Terminal
    public OutEventExample3 methodExample2(OutEventExample out) {
        System.out.println("methodExample2");
        return out != null ? new OutEventExample3(out.id) : null;
    }

}
