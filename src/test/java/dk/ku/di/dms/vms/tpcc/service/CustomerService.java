package dk.ku.di.dms.vms.tpcc.service;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.infra.QueryBuilder;
import dk.ku.di.dms.vms.infra.QueryBuilderFactory;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderOut;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderIn;
import dk.ku.di.dms.vms.tpcc.repository.ICustomerRepository;
import org.javatuples.Triplet;

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
        String sql = builder.select("c_discount, c_last, c_credit")
                            .from("customer")
                            .where("c_w_id = ", in.c_w_id)
                            .and("c_d_id = ", in.c_d_id)
                            .and("c_id = ", in.c_id)
                            .build();

        // TODO make query builder part of the repository
        Triplet<Float,String,String> customerData =
                (Triplet<Float, String, String>) customerRepository.fetch(sql);

        CustomerNewOrderOut customerTaxData = new CustomerNewOrderOut();
        customerTaxData.c_discount = customerData.getValue0();
        // these are not used anywhere...
        customerTaxData.c_last = customerData.getValue1();
        customerTaxData.c_credit = customerData.getValue2();
        // simply forwarding
        customerTaxData.c_id = in.c_id;

        return customerTaxData;
    }

}
