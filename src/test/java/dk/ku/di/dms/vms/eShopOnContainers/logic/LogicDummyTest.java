package dk.ku.di.dms.vms.eShopOnContainers.logic;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.eShopOnContainers.entity.Product;
import dk.ku.di.dms.vms.eShopOnContainers.events.AddProductRequest;
import dk.ku.di.dms.vms.eShopOnContainers.events.CheckoutRequest;
import dk.ku.di.dms.vms.eShopOnContainers.events.CheckoutStarted;
import dk.ku.di.dms.vms.eShopOnContainers.events.ProductAdded;
import dk.ku.di.dms.vms.eShopOnContainers.repository.IProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dk.ku.di.dms.vms.enums.IsolationLevelEnum.SERIALIZABLE;
import static dk.ku.di.dms.vms.enums.TransactionTypeEnum.RW;

public class LogicDummyTest {

    final private static Logger log = LoggerFactory.getLogger(LogicDummyTest.class);

    final private IProductRepository productRepository;

    public LogicDummyTest(final IProductRepository productRepository){
        this.productRepository = productRepository;
    }

    // inbound / outbound works as in kafka for spring

    @Inbound(values="add-product-request")
    @Outbound("product-added")
    @Transactional(type=RW, isolation=SERIALIZABLE)
    public ProductAdded addProduct(AddProductRequest addProductRequest){

        Product product = new Product(3.45,"nada","algum");

        productRepository.insert( product );

        return null;
    }

    @Inbound(values = "checkout-requests")
    @Outbound("accepted-checkouts")
    @Transactional(type=RW, isolation=SERIALIZABLE)
    public CheckoutStarted checkoutCart(CheckoutRequest checkoutRequest){
        // TODO finish
        log.info("I am executing, i am alive!!!");
        // send(CheckoutStarted.b)
        return null;
    }

}
