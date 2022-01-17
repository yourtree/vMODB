package dk.ku.di.dms.vms.tpcc.service;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.annotations.Outbound;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.infra.QueryBuilder;
import dk.ku.di.dms.vms.infra.QueryBuilderFactory;
import dk.ku.di.dms.vms.tpcc.events.WareDistNewOrderIn;
import dk.ku.di.dms.vms.tpcc.events.WareDistNewOrderOut;
import dk.ku.di.dms.vms.tpcc.repository.waredist.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.repository.waredist.IWarehouseRepository;
import org.javatuples.Pair;

import java.util.concurrent.*;

@Microservice("warehouse")
public class WareDistService {

    private final IWarehouseRepository warehouseRepository;
    private final IDistrictRepository districtRepository;

    public WareDistService(IWarehouseRepository warehouseRepository, IDistrictRepository districtRepository){
        this.warehouseRepository = warehouseRepository;
        this.districtRepository = districtRepository;
    }

    /**
     * TODO: which event should be processed first in the transaction?
     *       the query or district update?
     *       the case for constrains over events:
     *       (i) A < B (ii) B < A
     *       (iii) it does not matter the order of processing, but no concurrent
     *       (iv) order does not matter and can run concurrently
      */
    @Inbound(values = "waredist-new-order-in")
    @Outbound("waredist-new-order-out")
    @Transactional
    public WareDistNewOrderOut provideTaxInfo(WareDistNewOrderIn in)
            throws ExecutionException, InterruptedException {

        // create two threads, one for each query
        ExecutorService exec = Executors.newFixedThreadPool(2);

        Callable<Pair<Integer, Float>> r1 = () -> districtRepository.getNextOidAndTax(in.d_w_id, in.d_id);
        Future<Pair<Integer, Float>> fut1 = exec.submit(r1);

        Callable<Float> r2 = () -> warehouseRepository.getWarehouseTax(in.d_w_id);
        Future<Float> fut2 = exec.submit(r2);

        Pair<Integer, Float> getNextOidAndTax = fut1.get();
        Float tax = fut2.get();

        exec.shutdown();

        WareDistNewOrderOut out = new WareDistNewOrderOut(
                tax,
                getNextOidAndTax.getValue1(),
                getNextOidAndTax.getValue0(),
                in.d_w_id,
                in.d_id
        );

        return out;
    }

    @Inbound(values = "waredist-new-order-in")
    @Transactional
    public void processDistrictUpdate(WareDistNewOrderIn districtUpdateRequest){

        // repository query, much simpler
        /*
        QueryBuilderFactory.QueryBuilder builder = QueryBuilderFactory.init();
        String sql = builder.select("d_next_o_id, d_tax")
                .from("district")
                .where("d_w_id = ", districtUpdateRequest.d_w_id)
                .and("d_id = ", districtUpdateRequest.d_id)
                .build();
        */

        Pair<Integer,Float> districtData = districtRepository
                .getNextOidAndTax(districtUpdateRequest.d_w_id, districtUpdateRequest.d_id);

        Integer nextOid = districtData.getValue0();

        QueryBuilder builder = QueryBuilderFactory.init();
        String sql = builder.update("district")
                .set("d_next_o_id = ",nextOid+1)
                .where("d_w_id = ", districtUpdateRequest.d_w_id)
                .and("d_id = ", districtUpdateRequest.d_id)
                .build();

        districtRepository.fetch( sql );

        return;

    }

}
