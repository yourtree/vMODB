//package dk.ku.di.dms.vms.sdk.embed.services;
//
//import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
//import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
//import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
//import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample1;
//
//import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;
//
//@Microservice("example3")
//public class MicroserviceExample3 {
//
//    private int count;
//
//    public MicroserviceExample3() {
//        this.count = 0;
//    }
//
//    @Inbound(values = {"out1"})
//    @Transactional(type=W)
//    public void methodExample2(OutputEventExample1 out1) {
//        this.count = this.count + out1.id;
//        System.out.println("I am microservice 3: out1 "+out1.id+" total "+this.count);
//    }
//
//}
