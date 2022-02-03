package dk.ku.di.dms.vms.tpcc.service;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.api.modb.BuilderException;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderOut;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderIn;
import dk.ku.di.dms.vms.tpcc.repository.ICustomerRepository;
import dk.ku.di.dms.vms.utils.CustomerInfoDTO;
import dk.ku.di.dms.vms.utils.Triplet;

import java.util.List;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum.EQUALS;

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

        // CustomerInfoDTO customerInfoDTO = new CustomerInfoDTO();

        IQueryBuilder builder = QueryBuilderFactory.init();
        IStatement sql = builder.select("c_discount, c_last, c_credit")//.into(CustomerInfoDTO.class)
                            .from("customer")
                            .where("c_w_id", EQUALS, in.c_w_id)
                            .and("c_d_id", EQUALS, in.c_d_id)
                            .and("c_id", EQUALS, in.c_id)
                            .build();

        builder.select("o_id, c_id, c_last, c_discount")
                .from("customer")
                .join("order").on( "o_c_id", EQUALS, "customer.c_id" )
                .join( "order_line" ).on( "o_l_o_id", EQUALS, "order.o_id" )
                .build();

        // TODO make query builder part of the repository
        // List<CustomerInfoDTO> customerInfoDTO =
        Triplet<Float,String,String> customerData =
                (Triplet<Float, String, String>) customerRepository.fetch(sql);

        CustomerNewOrderOut customerTaxData = new CustomerNewOrderOut();
        customerTaxData.c_discount = customerData.getFirst();
        // these are not used anywhere...
        customerTaxData.c_last = customerData.getSecond();
        customerTaxData.c_credit = customerData.getThird();
        // simply forwarding
        customerTaxData.c_id = in.c_id;

        return customerTaxData;
    }

}
