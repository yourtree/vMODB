package dk.ku.di.dms.vms.tpcc_monolithic;

import dk.ku.di.dms.vms.Utils;
import dk.ku.di.dms.vms.annotations.Microservice;
import dk.ku.di.dms.vms.tpcc.entity.*;
import dk.ku.di.dms.vms.tpcc.repository.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.repository.IItemRepository;
import dk.ku.di.dms.vms.tpcc.repository.IStockRepository;
import dk.ku.di.dms.vms.tpcc.repository.waredist.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.repository.waredist.IWarehouseRepository;

import java.util.ArrayList;
import java.util.List;

import static dk.ku.di.dms.vms.tpcc.workload.Constants.*;

@Microservice("loader")
public class SyntheticDataLoader {

    private IWarehouseRepository warehouseRepository;
    private IDistrictRepository districtRepository;
    private ICustomerRepository customerRepository;
    private IItemRepository itemRepository;
    private IStockRepository stockRepository;

    public SyntheticDataLoader(IWarehouseRepository warehouseRepository,
                               IDistrictRepository districtRepository,
                               ICustomerRepository customerRepository,
                               IItemRepository itemRepository,
                               IStockRepository stockRepository){
        this.warehouseRepository = warehouseRepository;
        this.districtRepository = districtRepository;
        this.customerRepository = customerRepository;
        this.itemRepository = itemRepository;
        this.stockRepository = stockRepository;
    }

    public void load(int num_ware, int max_items){

        List<Warehouse> warehouses = new ArrayList<>(num_ware);
        // creating warehouse(s)
        for(int i = 0; i < num_ware; i++){
            warehouses.add( new Warehouse( i+1, ((double) Utils.randomNumber(10, 20) / 100.0), 3000000.00 ) );
        }

        this.warehouseRepository.insertAll( warehouses );

        List<District> warehouseDistricts = new ArrayList<>(num_ware * DIST_PER_WARE);
        int d_next_o_id = 3001;

        // creating districts
        for(int i = 0; i < num_ware; i++){

            for(int j = 0; j < DIST_PER_WARE; j++){

                District district = new District(
                        j+1, // district
                        i+1, // warehouse
                        (( Utils.randomNumber(10, 20)) / 100.0),
                        30000.0,
                        d_next_o_id);

                this.districtRepository.insertAll( warehouseDistricts );

            }

        }

        List<Customer> customers = new ArrayList<>(num_ware + DIST_PER_WARE * DIST_PER_WARE);

        // creating customers
        for(int i = 0; i < num_ware; i++) {

            for(int j = 0; j < DIST_PER_WARE; j++){

                for (int l = 0; l < CUST_PER_DIST; l++) {

                    int c_id = l+1;

                    String c_last;
                    if (c_id <= 1000) {
                        c_last = Utils.lastName(c_id - 1);
                    } else {
                        c_last = Utils.lastName(Utils.nuRand(255, 0, 999));
                    }

                    String c_credit = Utils.randomNumber(0, 1) == 1 ? "GC" : "BC";

                    Customer customer = new Customer(
                            c_id,
                            j+1,
                            i+1,
                            ((Utils.randomNumber(0, 50)) / 100.0f),
                            c_last,
                            c_credit,
                            -10.0f,
                            10.0f);

                    customers.add(customer);

                }

            }
        }

        customerRepository.insertAll( customers );

        List<Item> items = new ArrayList<>(MAX_ITEMS);

        // creating items
        for(int i = 0; i < max_items; i++){

            int i_im_id = Utils.randomNumber(1, 10000);

            String i_name = Utils.makeAlphaString(14, 24);

            // FIXME embrace orig[]
            String i_data = Utils.makeAlphaString(26, 50);

            items.add( new Item( i+1,  i_im_id, i_name, ((Utils.randomNumber(100, 10000)) / 100.0f), i_data ) );
        }

        List<Stock> stock = new ArrayList<>(num_ware * max_items);

        // creating stock items
        for(int i = 0; i < num_ware; i++) {

            // creating stock for each item
            for(int j = 0; j < max_items; j++){

                int s_quantity = Utils.randomNumber(MIN_STOCK_QTY, MAX_STOCK_QTY);
                String s_data = Utils.makeAlphaString(26, 50);
                String s_dist = Utils.makeAlphaString(24, 24);

                stock.add( new Stock( j+1,i+1, s_quantity, s_data, s_dist ) );
            }

        }

        stockRepository.insertAll( stock );

    }

}
