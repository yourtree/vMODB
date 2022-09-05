package dk.ku.di.dms.vms.sdk.core.example;

import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

@Microservice("example")
public class MicroserviceExample {

    @Inbound(values = {"in"})
    @Outbound("out")
    @Transactional
    public EventExample methodExample(EventExample in) {
        return new EventExample(1);
    }

}
