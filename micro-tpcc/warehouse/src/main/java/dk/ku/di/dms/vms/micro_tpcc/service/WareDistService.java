package dk.ku.di.dms.vms.micro_tpcc.service;

import dk.ku.di.dms.vms.micro_tpcc.dto.DistrictInfoDTO;
import dk.ku.di.dms.vms.micro_tpcc.events.WareDistNewOrderIn;
import dk.ku.di.dms.vms.micro_tpcc.events.WareDistNewOrderOut;
import dk.ku.di.dms.vms.micro_tpcc.repository.IDistrictRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.IWarehouseRepository;
import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.modb.api.query.builder.InsertStatementBuilder;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.builder.UpdateStatementBuilder;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;

import java.util.concurrent.*;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum.EQUALS;

@Microservice("warehouse")
public class WareDistService {

    private final IWarehouseRepository warehouseRepository;
    private final IDistrictRepository districtRepository;

    public WareDistService(IWarehouseRepository warehouseRepository, IDistrictRepository districtRepository){
        this.warehouseRepository = warehouseRepository;
        this.districtRepository = districtRepository;
    }

    /**
     * TODO: which payload should be processed first in the transaction?
     *       the query or district update?
     *       the case for constrains over events:
     *       (i) A < B (ii) B < A
     *       (iii) it does not matter the order of processing, but no concurrent
     *       (iv) order does not matter and can run concurrently
     *
     *  For this transaction, does the scheduler guarantees the correctness because the provideTaxInfo
     *  has the TID info? No. The write may happen before and install the next_o_id + 1.
     *  What can we do?
     *      (i) Schedule all reads first, wait for them to complete and then schedule the writes.
     *      (ii)
      */
    @Inbound(values = "waredist-new-order-in")
    @Outbound("waredist-new-order-out")
    @Transactional(type = RW)
    public WareDistNewOrderOut provideTaxInfo(WareDistNewOrderIn in) {

        DistrictInfoDTO districtData = districtRepository.getNextOidAndTax(in.d_w_id(), in.d_id());
        Float tax = warehouseRepository.getWarehouseTax(in.d_w_id());

        UpdateStatementBuilder builder = QueryBuilderFactory.update();
        IStatement sql = builder.update("district")
                .set("d_next_o_id",districtData.d_next_o_id() + 1)
                .where("d_w_id", EQUALS, in.d_w_id())
                .and("d_id", EQUALS, in.d_id())
                .build();

        districtRepository.issue(sql);

//        InsertStatementBuilder builderInsert = new InsertStatementBuilder();
//        var insertSta = builderInsert.insert("this").into("tb").build();

        return new WareDistNewOrderOut(
                tax,
                districtData.d_tax(),
                districtData.d_next_o_id(),
                in.d_w_id(),
                in.d_id()
        );
    }

}
