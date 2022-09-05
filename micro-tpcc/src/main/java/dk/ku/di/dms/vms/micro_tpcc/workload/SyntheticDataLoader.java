package dk.ku.di.dms.vms.micro_tpcc.workload;

import dk.ku.di.dms.vms.micro_tpcc.entity.*;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.dms.vms.micro_tpcc.repository.ICustomerRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.IHistoryRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.IItemRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.IStockRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.order.INewOrderRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.order.IOrderLineRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.order.IOrderRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.waredist.IDistrictRepository;
import dk.ku.di.dms.vms.micro_tpcc.repository.waredist.IWarehouseRepository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Microservice("loader")
public class SyntheticDataLoader {

    private final IWarehouseRepository warehouseRepository;
    private final IDistrictRepository districtRepository;
    private final ICustomerRepository customerRepository;
    private final IHistoryRepository historyRepository;
    private final IItemRepository itemRepository;
    private final IStockRepository stockRepository;
    private final IOrderRepository orderRepository;
    private final INewOrderRepository newOrderRepository;
    private final IOrderLineRepository orderLineRepository;

    public SyntheticDataLoader(IWarehouseRepository warehouseRepository,
                               IDistrictRepository districtRepository,
                               ICustomerRepository customerRepository,
                               IHistoryRepository historyRepository,
                               IItemRepository itemRepository,
                               IStockRepository stockRepository,
                               IOrderRepository orderRepository,
                               INewOrderRepository newOrderRepository,
                               IOrderLineRepository orderLineRepository){
        this.warehouseRepository = warehouseRepository;
        this.districtRepository = districtRepository;
        this.customerRepository = customerRepository;
        this.historyRepository = historyRepository;
        this.itemRepository = itemRepository;
        this.stockRepository = stockRepository;
        this.orderRepository = orderRepository;
        this.newOrderRepository = newOrderRepository;
        this.orderLineRepository = orderLineRepository;
    }

    @Transactional
    public void load(int num_ware, int max_items) {

        List<Warehouse> warehouses = new ArrayList<>(num_ware);
        // creating warehouse(s)
        for (int i = 0; i < num_ware; i++) {
            warehouses.add(new Warehouse(i + 1, ((double) Utils.randomNumber(10, 20) / 100.0), 3000000.00));
        }

        this.warehouseRepository.insertAll(warehouses);

        List<District> districts = new ArrayList<>(num_ware * Constants.DIST_PER_WARE);
        int d_next_o_id = 3001;

        // creating districts
        for (int i = 0; i < num_ware; i++) {
            for (int j = 0; j < Constants.DIST_PER_WARE; j++) {
                districts.add( new District(
                        j + 1, // district
                        i + 1, // warehouse
                        ((Utils.randomNumber(10, 20)) / 100.0),
                        30000.0,
                        d_next_o_id) );
            }
        }

        this.districtRepository.insertAll(districts);

        List<Customer> customers = new ArrayList<>(num_ware + Constants.DIST_PER_WARE * Constants.DIST_PER_WARE);
        List<History> historyRecords = new ArrayList<>(num_ware + Constants.DIST_PER_WARE * Constants.DIST_PER_WARE);

        // generated given the lack of PK
        int hist_id = 0;

        // creating customers
        for (int i = 0; i < num_ware; i++) {

            for (int j = 0; j < Constants.DIST_PER_WARE; j++) {

                for (int l = 0; l < Constants.CUST_PER_DIST; l++) {

                    hist_id++;

                    int c_id = l + 1;

                    String c_first = Utils.makeAlphaString(8, 16);

                    String c_last;
                    if (c_id <= 1000) {
                        c_last = Utils.lastName(c_id - 1);
                    } else {
                        c_last = Utils.lastName(Utils.nuRand(255, 0, 999));
                    }

                    Date c_since = new Date();

                    String c_credit = Utils.randomNumber(0, 1) == 1 ? "GC" : "BC";

                    customers.add( new Customer(
                            c_id,
                            j + 1,
                            i + 1,
                            ((Utils.randomNumber(0, 50)) / 100.0f),
                            c_first,
                            c_last,
                            c_since,
                            c_credit,
                            -10.0f,
                            10.0f) );

                    // respective history
                    historyRecords.add(
                            new History(hist_id,
                                    c_id,
                                    j + 1,
                                    i + 1,
                                    j + 1,
                                    i + 1,
                                    c_since,
                                    10.0f,
                                    Utils.makeAlphaString(12, 24)
                            )
                    );

                }

            }
        }

        customerRepository.insertAll(customers);
        historyRepository.insertAll( historyRecords );

        List<Item> items = new ArrayList<>(max_items);

        // creating items
        for (int i = 0; i < max_items; i++) {

            int i_im_id = Utils.randomNumber(1, 10000);

            String i_name = Utils.makeAlphaString(14, 24);

            // FIXME embrace orig[]
            String i_data = Utils.makeAlphaString(26, 50);

            items.add(new Item(i + 1, i_im_id, i_name, ((Utils.randomNumber(100, 10000)) / 100.0f), i_data));
        }

        itemRepository.insertAll( items );

        List<Stock> stock = new ArrayList<>(num_ware * max_items);

        // creating stock items
        for (int i = 0; i < num_ware; i++) {

            // creating stock for each item
            for (int j = 0; j < max_items; j++) {

                int s_quantity = Utils.randomNumber(Constants.MIN_STOCK_QTY, Constants.MAX_STOCK_QTY);
                String s_data = Utils.makeAlphaString(26, 50);
                String s_dist = Utils.makeAlphaString(24, 24);

                stock.add(new Stock(j + 1, i + 1, s_quantity, s_data, s_dist));
            }

        }

        stockRepository.insertAll(stock);

        /*
         * ==================================================================+ |
         * Order-related tables
         * +==================================================================
         */

        int o_c_id;
        int o_carrier_id;
        int o_ol_cnt;
        Date date;

        int ol_i_id;
        int ol_supply_w_id;
        int ol_quantity;
        float ol_amount;
        String ol_dist_info;

        List<Order> orders = new ArrayList<>(num_ware * Constants.DIST_PER_WARE * Constants.CUST_PER_DIST);
        List<NewOrder> newOrders = new ArrayList<>(num_ware * Constants.DIST_PER_WARE * Constants.CUST_PER_DIST);
        List<OrderLine> orderLines = new ArrayList<>(num_ware * Constants.DIST_PER_WARE * Constants.CUST_PER_DIST * Constants.MAX_NUM_ITEMS);

        // for each warehouse
        for (int w_id = 1; w_id <= num_ware; w_id++) {
            // for each district
            for (int d_id = 1; d_id <= Constants.DIST_PER_WARE; d_id++) {
                // generate orders
                for (int o_id = 1; o_id <= Constants.ORD_PER_DIST; o_id++) {

                    o_c_id = Utils.randomNumber(1, Constants.CUST_PER_DIST);
                    o_carrier_id = Utils.randomNumber(1, 10);
                    o_ol_cnt = Utils.randomNumber(Constants.MIN_NUM_ITEMS, Constants.MAX_NUM_ITEMS);

                    date = new Date(System.currentTimeMillis());

                    if (o_id <= 2100) {
                        orders.add(new Order(o_id, d_id, w_id, o_c_id, date, o_carrier_id, o_ol_cnt, 1));
                    } else {
                        orders.add(new Order(o_id, d_id, w_id, o_c_id, date, 0, o_ol_cnt, 1));
                        newOrders.add(new NewOrder(o_id, d_id, w_id));
                    }

                    // order lines
                    for (int ol = 1; ol <= o_ol_cnt; ol++) {

                        ol_i_id = Utils.randomNumber(1, max_items);
                        ol_supply_w_id = w_id;
                        ol_quantity = Constants.DEFAULT_ITEM_QTY;
                        ol_dist_info = Utils.makeAlphaString(24, 24);

                        if (o_id <= 2100) {
                            ol_amount = (Utils.randomNumber(10, 10000)) / 100.0f;
                            orderLines.add(new OrderLine(o_id, d_id, w_id, ol, ol_i_id, ol_supply_w_id, date, ol_quantity, ol_amount, ol_dist_info));
                        } else {
                            ol_amount = 0.0f;
                            orderLines.add(new OrderLine(o_id, d_id, w_id, ol, ol_i_id, ol_supply_w_id, null, ol_quantity, ol_amount, ol_dist_info));
                        }

                    }

                }
            }

        }

        orderRepository.insertAll(orders);
        newOrderRepository.insertAll(newOrders);
        orderLineRepository.insertAll( orderLines );

    }

}
