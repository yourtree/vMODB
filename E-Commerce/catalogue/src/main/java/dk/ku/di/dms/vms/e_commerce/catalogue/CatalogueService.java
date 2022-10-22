package dk.ku.di.dms.vms.e_commerce.catalogue;

import dk.ku.di.dms.vms.e_commerce.common.entity.Item;
import dk.ku.di.dms.vms.e_commerce.common.events.*;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
    @Inbound(values = {"new-order-item-resource", "new-order-user-resource", "payment-response"})
    @Outbound("shipment-request")
    @Transactional(type = RW)
    public ShipmentRequest newOrder(
            NewOrderItemResponse newOrderItemResource,
            NewOrderUserResponse newOrderUserResource,
            PaymentResponse paymentResponse){

        if(!paymentResponse.authorized) return null;

        List<Long> keys = newOrderItemResource.items.stream().map(p -> p.productId ).collect(Collectors.toList());

        // get all from database
        List<Product> products = this.productRepository.lookupByKeys(keys);

        List<Item> itemsForShipment = new ArrayList<>(products.size());

        Product currProduct;
        Item currItem;

        // verify availability
        for(int i = 0; i < products.size(); i++){

            currProduct = products.get(i);
            currItem = newOrderItemResource.items.get(i);

            if(currItem.quantity > currProduct.count) {
                // mark for replenishment
                currProduct.toReplenish = currProduct.toReplenish + currItem.quantity;
            } else {
                // decrease item from stock
                currProduct.count = currProduct.count - currItem.quantity;
                itemsForShipment.add(new Item( currItem.productId, currItem.quantity, currItem.unitPrice ));
            }

        }

        // TODO increase total amount sold in the current day

        // update stock
        this.productRepository.updateAll( products ); // can be parallel inside the database

        // create shipment event... actually create a new order... other vmss will listen to that, like the shipment and recommender engine
        return new ShipmentRequest( itemsForShipment, newOrderUserResource.customer, paymentResponse.date, paymentResponse.orderId );
        
    }

    public Object replenishStock(Object object){

        // TODO think about it. case where a function can either return a or b as result...
        return null;

    }
    
}
