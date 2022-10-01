package dk.ku.di.dms.vms.playground.micro_tpcc;

/**
 * Creates the input for a NewOrder transaction.
 */
public class NewOrderTransactionFactory {

    public static NewOrderTransactionInput build(final Integer num_ware, final Integer max_items, Integer dist_per_ware){

        int dist_per_ware_ = Constants.DIST_PER_WARE;
        if (dist_per_ware != null){
            dist_per_ware_ = dist_per_ware;
        }

        final int num_ware_ = num_ware == null ? Constants.DEFAULT_NUM_WARE : num_ware;
        final int max_items_ = max_items == null ? Constants.MAX_ITEMS : max_items;

        // TODO implement table_num functionality later
        // local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)

        int w_id = Utils.randomNumber(1, num_ware_);
        int d_id = Utils.randomNumber(1, dist_per_ware_);
        int c_id = Utils.nuRand(1023, 1, Constants.CUST_PER_DIST);

        int ol_cnt = Utils.randomNumber(Constants.MIN_NUM_ITEMS, Constants.MAX_NUM_ITEMS);
        int all_local = 1;

        int rbk = 0;

        int[] itemid = new int[Constants.MAX_NUM_ITEMS];
        int[] supware = new int[Constants.MAX_NUM_ITEMS];
        int[] qty = new int[Constants.MAX_NUM_ITEMS];

        // TODO adjust to make use of all_local variable...

        for (int i = 0; i < ol_cnt; i++) {

            itemid[i] = Utils.nuRand(8191, 1, max_items_);

            if (Utils.randomNumber(1, 100) != 1) {
                supware[i] = w_id;
            } else {
                supware[i] = otherWare(num_ware_, w_id);
                all_local = 0;
            }

            qty[i] = Utils.randomNumber(Constants.MIN_ITEM_QTY, Constants.MAX_ITEM_QTY);
        }

        return new NewOrderTransactionInput(w_id, d_id, c_id, ol_cnt, itemid, supware, qty);
    }

    /*
     * produce the id of a valid warehouse other than home_ware
     * (assuming there is one)
     */
    public static int otherWare(int num_ware, int home_ware) {
        int tmp;

        if (num_ware == 1) return home_ware;
        while ((tmp = Utils.randomNumber(1, num_ware)) == home_ware) ;
        return tmp;
    }

    public static String getOrderPerWarehouseAndDistrictId(int w_id, int d_id){
        return w_id + "_" + d_id;
    }

}
