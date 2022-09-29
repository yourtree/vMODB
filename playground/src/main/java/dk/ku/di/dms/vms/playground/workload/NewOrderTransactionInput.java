package dk.ku.di.dms.vms.playground.workload;

public class NewOrderTransactionInput {

    public int w_id;
    public int d_id;
    public int c_id;

    public int ol_cnt;

    public int[] itemid;
    public int[] supware;
    public int[] qty;

    public NewOrderTransactionInput(int w_id, int d_id, int c_id, int ol_cnt, int[] itemid, int[] supware, int[] qty) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
        this.ol_cnt = ol_cnt;
        this.itemid = itemid;
        this.supware = supware;
        this.qty = qty;
    }

}
