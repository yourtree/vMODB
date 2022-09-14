package dk.ku.di.dms.vms.playground;

import dk.ku.di.dms.vms.modb.api.annotations.*;

@Microservice("example")
public class MicroserviceExample {

    @Inbound(values = {"in"})
    @Outbound("out")
    @Transactional
    @Terminal
    public EventExample methodExample(EventExample in) {
        return new EventExample(1);
    }

}
