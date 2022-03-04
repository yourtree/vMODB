package dk.ku.di.dms.vms.eShopOnContainers.logic;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.database.api.modb.BuilderException;
import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.eShopOnContainers.entity.Product;
import dk.ku.di.dms.vms.eShopOnContainers.events.AddProductRequest;
import dk.ku.di.dms.vms.eShopOnContainers.events.CheckoutRequest;
import dk.ku.di.dms.vms.eShopOnContainers.events.CheckoutStarted;
import dk.ku.di.dms.vms.eShopOnContainers.events.ProductAdded;
import dk.ku.di.dms.vms.eShopOnContainers.repository.IProductRepository;
import dk.ku.di.dms.vms.tpcc.dto.CustomerInfoDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum.EQUALS;
import static dk.ku.di.dms.vms.enums.IsolationLevelEnum.SERIALIZABLE;
import static dk.ku.di.dms.vms.enums.TransactionTypeEnum.RW;

public class DummyLogic {

    final private static Logger log = LoggerFactory.getLogger(DummyLogic.class);

    final private IProductRepository productRepository;

    public DummyLogic(final IProductRepository productRepository){
        this.productRepository = productRepository;
    }

    // inbound / outbound works as in kafka for spring

    @Inbound(values="add-product-request")
    @Outbound("product-added")
    @Transactional(type=RW, isolation=SERIALIZABLE)
    public ProductAdded addProduct(AddProductRequest addProductRequest){

        Product product = new Product(3.45,"nada","sku","algum");

        productRepository.insert( product );

        return null;
    }

    @Inbound(values = "checkout-requests")
    @Outbound("accepted-checkouts")
    @Transactional(type=RW, isolation=SERIALIZABLE)
    public CheckoutStarted checkoutCart(CheckoutRequest checkoutRequest) throws BuilderException {
        // TODO finish

        IQueryBuilder builder = QueryBuilderFactory.init();
        IStatement sql = builder.select("c_discount, c_last, c_credit")//.into(CustomerInfoDTO.class)
                .from("customer")
                .where("c_w_id", EQUALS, 1)
                .and("c_d_id", EQUALS, 1)
                .and("c_id", EQUALS, 1)
                .build();

        CustomerInfoDTO customerInfo = productRepository.<CustomerInfoDTO>fetch(sql, CustomerInfoDTO.class);

        log.info("I am executing, i am alive!!!");

        return new CheckoutStarted();
    }

}
