package dk.ku.di.dms.vms.micro_tpcc.service;

import dk.ku.di.dms.vms.sdk.core.annotations.Inbound;
import dk.ku.di.dms.vms.sdk.core.annotations.Microservice;
import dk.ku.di.dms.vms.sdk.core.annotations.Transactional;
import dk.ku.di.dms.vms.modb.common.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.common.query.builder.SelectStatementBuilder;
import dk.ku.di.dms.vms.modb.common.query.builder.UpdateStatementBuilder;
import dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;
import dk.ku.di.dms.vms.micro_tpcc.events.StockNewOrderIn;
import dk.ku.di.dms.vms.micro_tpcc.repository.IStockRepository;

import java.util.concurrent.CompletableFuture;

// https://stackoverflow.com/questions/1250643/how-to-wait-for-all-threads-to-finish-using-executorservice

@Microservice("stock")
public class StockService {

    private final IStockRepository stockRepository;

    public StockService(IStockRepository stockRepository){
        this.stockRepository = stockRepository;
    }

    @Inbound(values = "stock-new-order-in")
    @Transactional
    public void processNewOrderItems(StockNewOrderIn in) {

        int n = in.ol_cnt();

        CompletableFuture<?>[] futures = new CompletableFuture[n];
        for(int i = 0; i < n; i++){

            final int finalI = i;
            futures[finalI] = CompletableFuture.runAsync(() -> {

                SelectStatementBuilder builder = QueryBuilderFactory.select();
                IStatement sql = builder.select("s_quantity")
                        .from("stock")
                        .where("s_i_id", ExpressionTypeEnum.EQUALS, in.itemsIds()[finalI])
                        .and("s_w_id", ExpressionTypeEnum.EQUALS, in.supware()[finalI])
                        .build();


                int s_quantity = stockRepository.fetch(sql,Integer.class);

                Integer ol_quantity = in.quantity()[finalI];
                if(s_quantity > ol_quantity){
                    s_quantity = s_quantity - ol_quantity;
                } else {
                    s_quantity = s_quantity - ol_quantity + 91;
                }

                UpdateStatementBuilder updateBuilder = QueryBuilderFactory.update();
                IStatement update = updateBuilder.update("stock")
                        .set("s_quantity",s_quantity)
                        .where("s_i_id", ExpressionTypeEnum.EQUALS, in.itemsIds()[finalI])
                        .and("s_w_id", ExpressionTypeEnum.EQUALS, in.supware()[finalI])
                        .build();


                stockRepository.issue(update);
            });

        }

        CompletableFuture.allOf(futures).join();

    }

}
