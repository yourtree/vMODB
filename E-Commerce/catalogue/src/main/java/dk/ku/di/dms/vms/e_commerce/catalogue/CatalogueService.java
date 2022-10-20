package dk.ku.di.dms.vms.e_commerce.catalogue;

import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderItemResource;
import dk.ku.di.dms.vms.e_commerce.common.events.NewOrderUserResource;
import dk.ku.di.dms.vms.e_commerce.common.events.PaymentRequest;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("catalogue")
public class CatalogueService {

    private final IProductRepository productRepository;

    public CatalogueService(IProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    /**
     * Validate the items, whether they exist (not removed), their prices, (discounts too?)
     * @param newOrderItemResource received from cart
     * @param newOrderUserResource received from user
     */
    @Inbound(values = {"new-order-item-resource","new-order-user-resource"})
    @Outbound("payment-request")
    @Transactional(type = RW)
    public PaymentRequest newOrder(NewOrderItemResource newOrderItemResource, NewOrderUserResource newOrderUserResource){

        List<Long> keys = newOrderItemResource.items.stream().map(p -> p.id ).toList();

        // get all from database
        List<Product> products = this.productRepository.lookupByKeys(keys);
        boolean allAvailable = true;
        float totalPrice = 0;

        // verify availability and calculate total amount // maybe also calculate discount in the future?
        for(int i = 0; i < products.size(); i++){
            if(newOrderItemResource.items.get(i).quantity > products.get(i).count) {
                allAvailable = false;
                break;
            }
            totalPrice += newOrderItemResource.items.get(i).unitPrice;
            // remove product from stock
            products.get(i).count = products.get(i).count - newOrderItemResource.items.get(i).quantity;
        }

        if(allAvailable){

            // update stock
            this.productRepository.updateAll( products ); // can be parallel inside the database

            // create payment request
            return new PaymentRequest( totalPrice, newOrderUserResource.customer, newOrderUserResource.address, newOrderUserResource.card );
        }

        // if no available stock
        return null;
        
    }

    public Object replenishStock(Object object){

        // TODO think about it. case where a function can either return a or b as result...
        return null;

    }
    
}
