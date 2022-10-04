package dk.ku.di.dms.vms.eshop.logic;

import dk.ku.di.dms.vms.eshop.entity.Product;
import dk.ku.di.dms.vms.eshop.events.AddProductRequest;
import dk.ku.di.dms.vms.eshop.events.CheckoutRequest;
import dk.ku.di.dms.vms.eshop.events.CheckoutStarted;
import dk.ku.di.dms.vms.eshop.events.ProductAdded;
import dk.ku.di.dms.vms.eshop.repository.IProductRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.dms.vms.modb.api.enums.IsolationLevelEnum;
import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.builder.SelectStatementBuilder;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyLogic {

    final private static Logger log = LoggerFactory.getLogger(DummyLogic.class);

    final private IProductRepository productRepository;

    public DummyLogic(final IProductRepository productRepository){
        this.productRepository = productRepository;
    }

    // inbound / outbound works as in kafka for spring

    @Inbound(values="add-product-request")
    @Outbound("product-added")
    @Transactional(type= TransactionTypeEnum.RW, isolation= IsolationLevelEnum.SERIALIZABLE)
    public ProductAdded addProduct(AddProductRequest addProductRequest){

        Product product = new Product(3.45,"nada","sku","algum");

        productRepository.insert( product );

        return null;
    }

    @Inbound(values = "checkout-requests")
    @Outbound("accepted-checkouts")
    @Transactional(type= TransactionTypeEnum.RW, isolation= IsolationLevelEnum.SERIALIZABLE)
    public CheckoutStarted checkoutCart(CheckoutRequest checkoutRequest) {
        // TODO finish

        SelectStatementBuilder builder = QueryBuilderFactory.select();
        SelectStatement sql = builder.select("c_discount, c_last, c_credit")//.into(CustomerInfoDTO.class)
                .from("customer")
                .where("c_w_id", ExpressionTypeEnum.EQUALS, 1)
                .and("c_d_id", ExpressionTypeEnum.EQUALS, 1)
                .and("c_id", ExpressionTypeEnum.EQUALS, 1L) // FIXME analyzer must check the type!!!!
                .build();

        SelectStatementBuilder testBuilder = QueryBuilderFactory.select();
        testBuilder.sum("id").select("so").from("tb1").groupBy("col1","col2").build();

        //CustomerInfoDTO customerInfo = productRepository.<CustomerInfoDTO>fetch(sql, CustomerInfoDTO.class);

        log.info("I am executing, i am alive!!!");

        return new CheckoutStarted();
    }

}
