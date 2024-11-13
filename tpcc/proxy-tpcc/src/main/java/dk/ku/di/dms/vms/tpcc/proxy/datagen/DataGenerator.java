package dk.ku.di.dms.vms.tpcc.proxy.datagen;

import dk.ku.di.dms.vms.tpcc.inventory.entities.Item;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Warehouse;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.*;

public final class DataGenerator {

    public static Map<Integer, Item> generateItemTable()
    {
        return generateItemTable(TPCcConstants.NUM_ITEMS);
    }

    public static Map<Integer,Item> generateItemTable(int numItems){
        var items = new HashMap<Integer, Item>();
        for (int I_ID = 1; I_ID <= numItems; I_ID ++)
        {
            items.put(I_ID, generateItem(I_ID));
        }
        return items;
    }

    public static Item generateItem(int I_ID) {
        int I_IM_ID = randomNumber(1, 10000);
        String I_NAME = makeAlphaString(14, 24);
        float I_PRICE = (float) ((randomNumber(100, 10000)) / 100.0);
        String I_DATA = makeAlphaString(26, 50);
        return new Item(I_ID, I_IM_ID, I_PRICE, I_NAME, I_DATA);
    }

    public static Warehouse generateWarehouse(int W_ID)
    {
        var W_NAME = makeAlphaString(6, 10);
        var W_STREET_1 = makeAlphaString(10, 20);
        var W_STREET_2 = makeAlphaString(10, 20);
        var W_CITY = makeAlphaString(10, 20);
        var W_STATE = makeAlphaString(2, 2);
        var W_ZIP = makeAlphaString(9, 9);
        var W_TAX = (float)((float) randomNumber(10, 20) / 100.0);
        var W_YTD = 3000000.00;
        return new Warehouse(W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD);
    }

    public static District generateDistrict(int D_ID, int D_W_ID)
    {
        String D_NAME = makeAlphaString(6, 10);
        var D_STREET_1 = makeAlphaString(10, 20);
        var D_STREET_2 = makeAlphaString(10, 20);
        var D_CITY = makeAlphaString(10, 20);
        var D_STATE = makeAlphaString(2, 2);
        var D_ZIP = makeAlphaString(9, 9);
        var D_TAX = (float) (((float) randomNumber(10, 20)) / 100.0);
        var D_YTD = (float) 30000.0;
        var D_NEXT_O_ID = 3001;
        return new District(D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID);
    }

    public static Map<Integer, Customer> generateCustomerTable(int c_d_id , int c_w_id)
    {
        var customer_table = new HashMap<Integer, Customer>();
        for (int C_ID = 1; C_ID <= TPCcConstants.NUM_CUST_PER_DIST; C_ID++)
        {
            Customer customer = generateCustomer(c_d_id, c_w_id, C_ID);
            customer_table.put(C_ID, customer);
        }
        return customer_table;
    }

    public static Customer generateCustomer(int c_d_id, int c_w_id, int C_ID) {
        String C_FIRST = makeAlphaString(8, 16);
        String C_MIDDLE = "O" + "E";
        String C_LAST;
        if (C_ID <= 1000) {
            C_LAST = lastName(C_ID - 1);
        } else {
            C_LAST = lastName(nuRand(255, 0, 999));
        }

        String C_STREET_1 = makeAlphaString(10, 20);
        String C_STREET_2 = makeAlphaString(10, 20);
        String C_CITY = makeAlphaString(10, 20);
        String C_STATE = makeAlphaString(2, 2);
        String C_ZIP = makeAlphaString(9, 9);
        String C_PHONE = makeNumberString(16, 16);
        Date C_SINCE = new Date();

        char C_CREDIT;
        if (randomNumber(0, 1) == 1)
            C_CREDIT = 'G';
        else
            C_CREDIT = 'B';
        C_CREDIT += 'C';

        int C_CREDIT_LIM = 50000;
        float C_DISCOUNT = (float) (((float) randomNumber(0, 50)) / 100.0);
        float C_BALANCE =  (float) -10.0;

        int C_YTD_PAYMENT = 10;
        int C_PAYMENT_CNT = 1;
        int C_DELIVERY_CNT = 0;
        String C_DATA = makeAlphaString(300, 500);

        return new Customer(C_ID, c_d_id, c_w_id, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA);
    }

    public static Map<Integer, Stock> generateStockTable(int w_id)
    {
        var stockMap = new HashMap<Integer, Stock>(TPCcConstants.NUM_ITEMS);
        for (int S_I_ID = 1; S_I_ID <= TPCcConstants.NUM_ITEMS; S_I_ID++)
        {
            Stock stock = generateStockItem(w_id, S_I_ID);
            stockMap.put(S_I_ID, stock);
        }
        return stockMap;
    }

    public static Stock generateStockItem(int w_id, int S_I_ID) {
        var S_QUANTITY = randomNumber(10, 100);
        var S_DIST = new HashMap<Integer, String>();
        for (int d = 1; d <= TPCcConstants.NUM_DIST_PER_WARE; d++) S_DIST.put(d, makeAlphaString(24, 24));
        int S_YTD = 0;
        int S_ORDER_CNT = 0;
        int S_REMOTE_CNT = 0;
        var S_DATA = makeAlphaString(26, 50);
        return new Stock(S_I_ID, w_id, S_QUANTITY, S_DIST, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA);
    }

}