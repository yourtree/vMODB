package dk.ku.di.dms.vms.tpcc.service;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.database.query.parse.Statement;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilder;
import dk.ku.di.dms.vms.database.api.modb.QueryBuilderFactory;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderOut;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderIn;
import dk.ku.di.dms.vms.tpcc.repository.ICustomerRepository;
import dk.ku.di.dms.vms.utils.Triplet;

import static dk.ku.di.dms.vms.database.query.parse.ExpressionEnum.EQUALS;

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
    public CustomerNewOrderOut provideCustomerDataToOrder(CustomerNewOrderIn in){

        QueryBuilder builder = QueryBuilderFactory.init();
        Statement sql = builder.select("c_discount, c_last, c_credit")
                            .from("customer")
                            .where("c_w_id", EQUALS, in.c_w_id)
                            .and("c_d_id", EQUALS, in.c_d_id)
                            .and("c_id", EQUALS, in.c_id)
                            .build();

        // TODO make query builder part of the repository
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
