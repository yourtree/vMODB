package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.Arrays;

@Event
public final class NewOrderWareIn {

    public int w_id;
    public int d_id;
    public int c_id;
    public int[] itemsIds;
    public int[] supWares;
    public int[] qty;
    public boolean allLocal;

    @SuppressWarnings("unused")
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

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":\"" + w_id + "\""
                + ",\"d_id\":\"" + d_id + "\""
                + ",\"c_id\":\"" + c_id + "\""
                + ",\"itemsIds\":" + Arrays.toString(itemsIds)
                + ",\"supWares\":" + Arrays.toString(supWares)
                + ",\"qty\":" + Arrays.toString(qty)
                + ",\"allLocal\":\"" + allLocal + "\""
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NewOrderWareIn that = (NewOrderWareIn) o;

        if (w_id != that.w_id) return false;
        if (d_id != that.d_id) return false;
        if (c_id != that.c_id) return false;
        if (allLocal != that.allLocal) return false;
        /*
        if (!Arrays.equals(itemsIds, that.itemsIds)) return false;
        if (!Arrays.equals(supWares, that.supWares)) return false;
        return Arrays.equals(qty, that.qty);
         */
        int maxSize = Math.min(itemsIds.length, that.itemsIds.length);
        int idx = 0;
        while(idx < maxSize){
            if(itemsIds[idx] != that.itemsIds[idx]){
                return itemsIds[idx] == -1 || that.itemsIds[idx] == -1;
            }
            idx++;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = w_id;
        result = 31 * result + d_id;
        result = 31 * result + c_id;
        result = 31 * result + Arrays.hashCode(itemsIds);
        result = 31 * result + Arrays.hashCode(supWares);
        result = 31 * result + Arrays.hashCode(qty);
        result = 31 * result + (allLocal ? 1 : 0);
        return result;
    }

}