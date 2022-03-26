package dk.ku.di.dms.vms.micro_tpcc.service;

import dk.ku.di.dms.vms.micro_tpcc.dto.DistrictInfoDTO;
import dk.ku.di.dms.vms.modb.common.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.common.query.builder.UpdateStatementBuilder;
import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;
import dk.ku.di.dms.vms.sdk.core.annotations.Inbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Microservice;
import dk.ku.di.dms.vms.sdk.core.annotations.Outbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Transactional;
import dk.ku.di.dms.vms.micro_tpcc.events.WareDistNewOrderIn;
import dk.ku.di.dms.vms.micro_tpcc.events.WareDistNewOrderOut;
import dk.ku.di.dms.vms.micro_tpcc.repository.waredist.IDistrictRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.waredist.IWarehouseRepository;

import java.util.concurrent.*;

import static dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum.EQUALS;

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

        Callable<DistrictInfoDTO> r1 = () -> districtRepository.getNextOidAndTax(in.d_w_id(), in.d_id());
        Future<DistrictInfoDTO> fut1 = exec.submit(r1);

        Callable<Float> r2 = () -> warehouseRepository.getWarehouseTax(in.d_w_id());
        Future<Float> fut2 = exec.submit(r2);

        DistrictInfoDTO getNextOidAndTax = fut1.get();
        Float tax = fut2.get();

        exec.shutdown();

        WareDistNewOrderOut out = new WareDistNewOrderOut(
                tax,
                getNextOidAndTax.d_tax(),
                getNextOidAndTax.d_next_o_id(),
                in.d_w_id(),
                in.d_id()
        );

        return out;
    }

    @Inbound(values = "waredist-new-order-in")
    @Transactional
    public void processDistrictUpdate(WareDistNewOrderIn districtUpdateRequest) {

        // repository query, much simpler
        /*
        QueryBuilderFactory.QueryBuilder builder = QueryBuilderFactory.init();
        String sql = builder.select("d_next_o_id, d_tax")
                .from("district")
                .where("d_w_id = ", districtUpdateRequest.d_w_id)
                .and("d_id = ", districtUpdateRequest.d_id)
                .build();
        */

        DistrictInfoDTO districtData = districtRepository
                .getNextOidAndTax(districtUpdateRequest.d_w_id(), districtUpdateRequest.d_id());

        int nextOid = districtData.d_next_o_id();

        UpdateStatementBuilder builder = QueryBuilderFactory.update();
        IStatement sql = builder.update("district")
                .set("d_next_o_id",nextOid+1)
                .where("d_w_id", EQUALS, districtUpdateRequest.d_w_id())
                .and("d_id", EQUALS, districtUpdateRequest.d_id())
                .build();

        districtRepository.issue(sql);

    }

}
