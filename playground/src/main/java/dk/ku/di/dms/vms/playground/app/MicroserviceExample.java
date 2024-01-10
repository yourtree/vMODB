package dk.ku.di.dms.vms.playground.app;

import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("example")
public class MicroserviceExample {

    private final RepositoryExample repository;

    public MicroserviceExample(RepositoryExample repository){
        this.repository = repository;
    }

    @Inbound(values = {"in"})
    @Outbound("out")
    @Transactional(type=R)
    public OutEventExample methodExample(EventExample in) {
        System.out.println("[example] methodExample");
        return in != null ? new OutEventExample(in.id) : null;
    }

    @Inbound(values = {"in"})
    @Outbound("out2")
    @Transactional(type=RW)
    public OutEventExample2 methodExample1(EventExample in) {

        System.out.println("[example] methodExample1");

        // repository.getItemsById( new int[]{1,2,3} );

        repository.insert(new EntityExample(in.id, in.id));

        return new OutEventExample2(in.id);
    }

}
