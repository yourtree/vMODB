package dk.ku.di.dms.vms.micro_tpcc.service;

import dk.ku.di.dms.vms.micro_tpcc.events.StockNewOrderOut;
import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.builder.SelectStatementBuilder;
import dk.ku.di.dms.vms.modb.api.query.builder.UpdateStatementBuilder;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.api.interfaces.IDTO;
import dk.ku.di.dms.vms.micro_tpcc.events.StockNewOrderIn;
import dk.ku.di.dms.vms.micro_tpcc.repository.IStockRepository;

import java.util.concurrent.CompletableFuture;

import static dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum.EQUALS;

/**
 * https://stackoverflow.com/questions/1250643/how-to-wait-for-all-threads-to-finish-using-executorservice
 *
 * I believe it is worthy to create my own executor future abstraction so, I can handle myself the independent tasks setup by the user
 *
 */
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

            stockRepository.submit(() -> System.out.println("TESTE"));

            futures[finalI] = CompletableFuture.runAsync( new @Disjoint Runnable() {

              @Override
              public void run() {
                SelectStatementBuilder builder = QueryBuilderFactory.select();
                SelectStatement sql = builder.select("s_quantity")
                        .from("stock")
                        .where("s_i_id", EQUALS, in.itemsIds()[finalI])
                        .and("s_w_id", EQUALS, in.supware()[finalI])
                        .build();

                int s_quantity = stockRepository.fetchOne(sql,Integer.class);

                int ol_quantity = in.quantity()[finalI];
                if(s_quantity > ol_quantity){
                    s_quantity = s_quantity - ol_quantity;
                } else {
                    s_quantity = s_quantity - ol_quantity + 91;
                }

                UpdateStatementBuilder updateBuilder = QueryBuilderFactory.update();
                IStatement update = updateBuilder.update("stock")
                        .set("s_quantity", s_quantity)
                        .where("s_i_id", EQUALS, in.itemsIds()[finalI])
                        .and("s_w_id", EQUALS, in.supware()[finalI])
                        .build();


                stockRepository.issue(update);
              }
            });

        }

        CompletableFuture.allOf(futures).join();

    }

    private static class DistInfoDTO implements IDTO {
        public int s_i_id;
        public String s_dist;
    }

    @Inbound(values = "stock-new-order-in")
    @Outbound(value = "stock-new-order-out")
    @Transactional
    public StockNewOrderOut getItemsDistributionInfo(StockNewOrderIn in){

        int size = in.ol_cnt();
        int[] itemIds = new int[size];
        String[] itemsDistInfo = new String[size];

        for(int i = 0; i < size; i++) {

            SelectStatementBuilder builder = QueryBuilderFactory.select();
            SelectStatement sql = builder.select("s_i_id, s_dist")//.into(CustomerInfoDTO.class)
                    .from("stock")
                    .where("s_i_id", EQUALS, in.itemsIds()[i])
                    .and("w_i_id", EQUALS, in.supware()[i])
                    .build();

            DistInfoDTO dto = stockRepository.fetchOne(sql, DistInfoDTO.class);
            itemIds[i] = dto.s_i_id;
            itemsDistInfo[i] = dto.s_dist;

        }

        return new StockNewOrderOut(itemIds, itemsDistInfo);

    }

}
