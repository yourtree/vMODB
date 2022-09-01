package dk.ku.di.dms.vms.micro_tpcc.service;

import dk.ku.di.dms.vms.modb.common.interfaces.application.IVmsFuture;
import dk.ku.di.dms.vms.modb.common.query.statement.SelectStatement;
import dk.ku.di.dms.vms.sdk.core.annotations.Inbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Microservice;
import dk.ku.di.dms.vms.sdk.core.annotations.Outbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Transactional;
import dk.ku.di.dms.vms.modb.common.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.common.query.builder.SelectStatementBuilder;
import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;
import dk.ku.di.dms.vms.micro_tpcc.events.CustomerNewOrderOut;
import dk.ku.di.dms.vms.micro_tpcc.events.CustomerNewOrderIn;
import dk.ku.di.dms.vms.micro_tpcc.repository.ICustomerRepository;
import dk.ku.di.dms.vms.micro_tpcc.dto.CustomerInfoDTO;

import static dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum.EQUALS;

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
    public CustomerNewOrderOut provideCustomerDataToOrder(CustomerNewOrderIn in) {

        SelectStatementBuilder builder = QueryBuilderFactory.select();
        SelectStatement sql = builder.select("c_discount, c_last, c_credit")//.into(CustomerInfoDTO.class)
                            .from("customer")
                            .where("c_w_id", EQUALS, in.c_w_id())
                            .and("c_d_id", EQUALS, in.c_d_id())
                            .and("c_id", EQUALS, in.c_id())
                            .build();

//        builder.select("o_id, c_id, c_last, c_discount")
//                .from("customer")
//                .join("order","o_c_id").on(EQUALS, "customer.c_id" )
//                .join( "order_line", "o_l_o_id" ).on(EQUALS, "order.o_id" )
//                .build();

        // TODO make query builder part of the repository

        // do we hide or expose the async primitives to the user?

        // somebody needs to fulfill this promise. a vms future guarantees the get has no problems
        IVmsFuture<CustomerInfoDTO> futureCustomerInfo = customerRepository.<CustomerInfoDTO>fetchOnePromise(sql, CustomerInfoDTO.class);
        // get is blocking, but also isDone()
        CustomerInfoDTO customerInfoFromFuture = futureCustomerInfo.get();

        CustomerInfoDTO customerInfo = customerRepository.<CustomerInfoDTO>fetchOne(sql, CustomerInfoDTO.class);
        // the user issues get, the task will block
        // the thread goes back to the pool e will be allocated to other tasks that are available to run

        return new CustomerNewOrderOut(customerInfo.c_discount(),
                customerInfo.c_last(),customerInfo.c_credit(),in.c_id());
    }

}
