package dk.ku.di.dms.vms.e_commerce.catalogue;

import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderItemResource;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderResult;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderUserResource;
import dk.ku.di.dms.vms.e_commerce.common.events.PaymentRequest;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;

@Microservice("catalogue")
public class CatalogueService {

    private final IProductRepository productRepository;

    public CatalogueService(IProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    /**
     * Calidate the items, whether they exist (not removed), their prices, (discounts too?)
     * @param newOrderItemResource received from cart
     * @param newOrderUserResource received from user
     */
    public PaymentRequest newOrder(NewOrderItemResource newOrderItemResource, NewOrderUserResource newOrderUserResource){

        return null;
        
    }

    public Object newOrder(NewOrderResult newOrderResult ){

        return null;

    }
    
}
