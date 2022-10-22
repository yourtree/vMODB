package dk.ku.di.dms.vms.e_commerce.user;

import dk.ku.di.dms.vms.e_commerce.common.entity.Customer;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderRequest;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderUserResponse;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;

@Microservice("user")
public class UserService {

    private final IUserRepository userRepository;

    private final ICardRepository cardRepository;

    private final IAddressRepository addressRepository;

    public UserService(IUserRepository userRepository, ICardRepository cardRepository, IAddressRepository addressRepository) {
        this.userRepository = userRepository;
        this.cardRepository = cardRepository;
        this.addressRepository = addressRepository;
    }

    @Inbound(values = {"new-order-user"})
    @Outbound("new-order-user-resource")
    @Transactional(type = R)
    public NewOrderUserResponse newOrder(NewOrderRequest newOrderUserRequest){

        User user = userRepository.findByCustomerId(newOrderUserRequest.customerId);
        Card card = cardRepository.lookupByKey( newOrderUserRequest.cardId );
        Address address = addressRepository.lookupByKey( newOrderUserRequest.addressId );

        Customer customer = new Customer();
        customer.customerId = user.id;
        customer.firstName = user.firstName;
        customer.lastName = user.lastName;
        customer.username = user.username;
        customer.street = address.street;
        customer.number = address.number;
        customer.country = address.country;
        customer.city = address.city;
        customer.postCode =  address.postCode;
        customer.longNum = card.longNum;
        customer.expires = card.expires;
        customer.ccv = card.ccv;
        
        return new NewOrderUserResponse(customer);

    }


}
