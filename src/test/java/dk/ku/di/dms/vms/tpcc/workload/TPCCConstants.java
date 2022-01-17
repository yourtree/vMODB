package dk.ku.di.dms.vms.tpcc.workload;

public class TPCCConstants {

    /* TABLE IDENTIFIERS TO MATCH ACTOR PLACEMENT */
    public static int WARE = 1;
    public static int DIST = 2;
    public static int CUST = 3; // not necessary since warehouse and customer actors are together
    public static int HIST = 4;
    public static int ORDER = 5;
    public static int NEW_ORD = 6;
    public static int ORD_LN = 7;
    public static int STOCK = 8;
    public static int ITEM = 9;

    /* definitions for new order transaction */
    public static int MIN_NUM_ITEMS = 5;
    public static int MAX_NUM_ITEMS = 15;

    public static int MIN_ITEM_QTD = 1;
    public static int MAX_ITEM_QTD = 10;

    public static int MAX_ITEMS = 100000;
    public static int DIST_PER_WARE = 10;
    public static int CUST_PER_DIST = 3000;

    public static int DEFAULT_NUM_WARE = 1; //8;

    public static int DEFAULT_STOCK_QTD = 1000;
    
}
