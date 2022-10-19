package dk.ku.di.dms.vms.e_commerce.cart;

import dk.ku.di.dms.vms.modb.api.annotations.Microservice;

@Microservice("cart")
public class CartService {

    private final ICartRepository cartRepository;

    public CartService(ICartRepository cartRepository){
        this.cartRepository = cartRepository;
    }

}
