package dk.ku.di.dms.vms.sdk.core.example;

import dk.ku.di.dms.vms.sdk.core.annotations.Inbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Microservice;
import dk.ku.di.dms.vms.sdk.core.annotations.Outbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Transactional;

@Microservice("example")
public class MicroserviceExample {

    @Inbound(values = {"in"})
    @Outbound("out")
    @Transactional
    public EventExample methodExample(EventExample in) {
        return new EventExample(1);
    }

}
