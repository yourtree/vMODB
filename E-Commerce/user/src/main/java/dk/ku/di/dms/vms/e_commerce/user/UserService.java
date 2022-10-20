package dk.ku.di.dms.vms.e_commerce.user;

import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderItemResource;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderUserResource;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;

@Microservice("user")
public class UserService {

    private final IUserRepository userRepository;

    public UserService(IUserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Inbound(values = {"new-order-user"})
    @Outbound("new-order-user-resource")
    @Transactional(type = R)
    public NewOrderUserResource newOrder(Long customerId){

        // TODO finish
        return null;

    }


}
