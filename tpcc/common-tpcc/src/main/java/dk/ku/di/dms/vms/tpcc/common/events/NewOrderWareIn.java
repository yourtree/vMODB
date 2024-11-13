package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class NewOrderWareIn {

    public int w_id;
    public int d_id;
    public int c_id;
    public int[] itemsIds;
    public int[] supWares;
    public int[] qty;
    public boolean allLocal;

    public NewOrderWareIn(){}

    public NewOrderWareIn(int w_id, int d_id, int c_id, int[] itemsIds, int[] supWares, int[] qty, boolean allLocal) {
            this.w_id = w_id;
            this.d_id = d_id;
            this.c_id = c_id;
            this.itemsIds = itemsIds;
            this.supWares = supWares;
            this.qty = qty;
            this.allLocal = allLocal;
    }

    public record WareDistId(int w_id, int d_id){}

    @SuppressWarnings("unused")
    public NewOrderWareIn.WareDistId getId(){
        return new NewOrderWareIn.WareDistId(this.w_id, this.d_id);
    }

}