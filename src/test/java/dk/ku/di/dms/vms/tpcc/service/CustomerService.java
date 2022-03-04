package dk.ku.di.dms.vms.tpcc.service;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.api.modb.BuilderException;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderOut;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderIn;
import dk.ku.di.dms.vms.tpcc.repository.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.dto.CustomerInfoDTO;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum.EQUALS;

@Microservice("customer")
public class CustomerService {

    private final ICustomerRepository customerRepository;

    public CustomerService(ICustomerRepository customerRepository){
        this.customerRepository = customerRepository;
    }

    // this reminds a Command and its respective command handler
    @Inbound(values = {"customer-new-order-in"})
    @Outbound("customer-new-order-out")
    @Transactional
    public CustomerNewOrderOut provideCustomerDataToOrder(CustomerNewOrderIn in) throws BuilderException {

        IQueryBuilder builder = QueryBuilderFactory.init();
        IStatement sql = builder.select("c_discount, c_last, c_credit")//.into(CustomerInfoDTO.class)
                            .from("customer")
                            .where("c_w_id", EQUALS, in.c_w_id)
                            .and("c_d_id", EQUALS, in.c_d_id)
                            .and("c_id", EQUALS, in.c_id)
                            .build();

//        builder.select("o_id, c_id, c_last, c_discount")
//                .from("customer")
//                .join("order","o_c_id").on(EQUALS, "customer.c_id" )
//                .join( "order_line", "o_l_o_id" ).on(EQUALS, "order.o_id" )
//                .build();

        // TODO make query builder part of the repository
        // List<CustomerInfoDTO> customerInfoDTO =
        //Triplet<Float,String,String> customerData =
        // CustomerInfoDTO customerInfo = (CustomerInfoDTO) customerRepository.fetch(sql, CustomerInfoDTO.class);
        CustomerInfoDTO customerInfo = customerRepository.<CustomerInfoDTO>fetch(sql, CustomerInfoDTO.class);
        // SqlRepository.fetch( customerInfo, sql );

        CustomerNewOrderOut customerTaxData = new CustomerNewOrderOut();
        customerTaxData.c_discount = customerInfo.c_discount;
        // these are not used anywhere...
        customerTaxData.c_last = customerInfo.c_last;
        customerTaxData.c_credit = customerInfo.c_credit;
        // simply forwarding
        customerTaxData.c_id = in.c_id;

        return customerTaxData;
    }

}
