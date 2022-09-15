package dk.ku.di.dms.vms.playground.app;

import dk.ku.di.dms.vms.modb.api.annotations.*;

@Microservice("example")
public class MicroserviceExample {

    @Inbound(values = {"in"})
    @Outbound("out")
    @Transactional
    @Terminal
    public OutEventExample methodExample(EventExample in) {
        System.out.println("I am alive. The scheduler has scheduled me successfully!");
        return in != null ? new OutEventExample(in.id) : null;
    }

}
