package dk.ku.di.dms.vms.tpcc.service;

import dk.ku.di.dms.vms.annotations.Inbound;
import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.annotations.Transactional;
import dk.ku.di.dms.vms.infra.QueryBuilder;
import dk.ku.di.dms.vms.infra.QueryBuilderFactory;
import dk.ku.di.dms.vms.tpcc.events.StockNewOrderIn;
import dk.ku.di.dms.vms.tpcc.repository.IStockRepository;

import java.util.concurrent.*;

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
        int n = in.ol_cnt;
        //ExecutorService exec = Executors.newFixedThreadPool(n);
        CompletableFuture<?>[] futures = new CompletableFuture[n];
        for(int i = 0; i < n; i++){

            final int finalI = i;
            futures[finalI] = CompletableFuture.runAsync(() -> {

                QueryBuilder builder = QueryBuilderFactory.init();
                String sql = builder.select("s_quantity")
                        .from("stock")
                        .where("s_i_id = ", in.itemsIds.get(finalI))
                        .and("s_w_id = ", in.supware.get(finalI))
                        .build();

                Integer s_quantity = (Integer) stockRepository.fetch(sql);

                Integer ol_quantity = in.quantity.get(finalI);
                if(s_quantity > ol_quantity){
                    s_quantity = s_quantity - ol_quantity;
                } else {
                    s_quantity = s_quantity - ol_quantity + 91;
                }

                String update = builder.update("stock")
                        .set("s_quantity = ",s_quantity)
                        .where("s_i_id = ", in.itemsIds.get(finalI))
                        .and("s_w_id = ", in.supware.get(finalI))
                        .build();

                stockRepository.fetch(update);
            });

        }

        CompletableFuture.allOf(futures).join();
        //exec.shutdown();

    }



}
