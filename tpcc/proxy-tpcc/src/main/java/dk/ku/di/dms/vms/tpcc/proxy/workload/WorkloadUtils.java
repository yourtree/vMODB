package dk.ku.di.dms.vms.tpcc.proxy.workload;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;

import java.util.ArrayList;
import java.util.List;

import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.nuRand;
import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.randomNumber;
import static dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants.*;

public final class WorkloadUtils {

    private static final Schema SCHEMA = new Schema(
            new String[]{ "w_id", "d_id", "c_id", "itemIds", "supWares", "qty", "allLocal" },
            new DataType[]{
                    DataType.INT, DataType.INT, DataType.INT, DataType.INT_ARRAY,
                    DataType.INT_ARRAY, DataType.INT_ARRAY, DataType.BOOL
            },
            new int[]{},
            new ConstraintReference[]{},
            false
    );

    private static void write(long pos, Object[] record) {
        long currAddress = pos;
        for (int index = 0; index < SCHEMA.columnOffset().length; index++) {
            DataType dt = SCHEMA.columnDataType(index);
            DataTypeUtils.callWriteFunction(currAddress, dt, record[index]);
            currAddress += dt.value;
        }
    }

    private static Object[] read(long address){
        Object[] record = new Object[SCHEMA.columnOffset().length];
        long currAddress = address;
        for(int i = 0; i < SCHEMA.columnOffset().length; i++) {
            DataType dt = SCHEMA.columnDataType(i);
            record[i] = DataTypeUtils.getValue(dt, currAddress);
            currAddress += dt.value;
        }
        return record;
    }

    public static void submitWorkload(List<NewOrderWareIn> input){



        return;
    }

    public static List<NewOrderWareIn> loadWorkloadData(){
        List<NewOrderWareIn> input = new ArrayList<>(NUM_TRANSACTIONS);
        AppendOnlyBuffer buffer = EmbedMetadataLoader.loadAppendOnlyBuffer(NUM_TRANSACTIONS, SCHEMA.getRecordSize(),"new_order_input", false);
        for(int txIdx = 1; txIdx <= NUM_TRANSACTIONS; txIdx++) {
            Object[] newOrderInput = read(buffer.nextOffset());
            input.add(parseRecordIntoEntity(newOrderInput));
            buffer.forwardOffset(SCHEMA.getRecordSize());
        }
        return input;
    }

    public static List<NewOrderWareIn> createWorkload(){
        List<NewOrderWareIn> input = new ArrayList<>(NUM_TRANSACTIONS);
        AppendOnlyBuffer buffer = EmbedMetadataLoader.loadAppendOnlyBuffer(NUM_TRANSACTIONS, SCHEMA.getRecordSize(),"new_order_input", true);
        for(int txIdx = 1; txIdx <= NUM_TRANSACTIONS; txIdx++) {
            int w_id = randomNumber(1, NUM_WARE);
            Object[] newOrderInput = generateNewOrder(w_id, NUM_WARE);
            input.add(parseRecordIntoEntity(newOrderInput));
            write(buffer.nextOffset(), newOrderInput);
            buffer.forwardOffset(SCHEMA.getRecordSize());
        }
        buffer.force();
        return input;
    }

    private static NewOrderWareIn parseRecordIntoEntity(Object[] newOrderInput) {
        return new NewOrderWareIn(
                (int) newOrderInput[0],
                (int) newOrderInput[1],
                (int) newOrderInput[2],
                (int[]) newOrderInput[3],
                (int[]) newOrderInput[4],
                (int[]) newOrderInput[5],
                (boolean) newOrderInput[6]
        );
    }

    private static Object[] generateNewOrder(int w_id, int num_ware){
        int d_id;
        int c_id;
        int ol_cnt;
        int all_local = 1;
        int not_found = NUM_ITEMS + 1;
        int rbk;

        int max_num_items_per_order_ = Math.min(MAX_NUM_ITEMS_PER_ORDER, NUM_ITEMS);
        int min_num_items_per_order_ = Math.min(5, max_num_items_per_order_);

        d_id = randomNumber(1, NUM_DIST_PER_WARE);
        c_id = nuRand(1023, 1, NUM_CUST_PER_DIST);

        ol_cnt = randomNumber(min_num_items_per_order_, max_num_items_per_order_);
        rbk = randomNumber(1, 100);

        int[] itemIds = new int[ol_cnt];
        int[] supWares = new int[ol_cnt];
        int[] qty = new int[ol_cnt];

        for (int i = 0; i < ol_cnt; i++) {
            int item_ = nuRand(8191, 1, NUM_ITEMS);

            // avoid duplicate items
            while(foundItem(itemIds, i, item_)){
                item_ = nuRand(8191, 1, NUM_ITEMS);
            }
            itemIds[i] = item_;

            if(FORCE_ABORTS) {
                if ((i == ol_cnt - 1) && (rbk == 1)) {
                    // this can lead to exception and then abort in app code
                    itemIds[i] = not_found;
                }
            }

            if (ALLOW_MULTI_WAREHOUSE_TX) {
                if (randomNumber(1, 100) != 1) {
                    supWares[i] = w_id;
                } else {
                    supWares[i] = otherWare(num_ware, w_id);
                    all_local = 0;
                }
            } else {
                supWares[i] = w_id;
            }
            qty[i] = randomNumber(1, 10);
        }

        return new Object[]{ w_id, d_id, c_id, itemIds, supWares, qty, all_local == 0 };
    }

    private static boolean foundItem(int[] itemIds, int length, int value){
        if(length == 0) return false;
        for(int i = 0; i < length; i++){
            if(itemIds[i] == value) return true;
        }
        return false;
    }

    /**
     * Based on <a href="https://github.com/AgilData/tpcc/blob/master/src/main/java/com/codefutures/tpcc/Driver.java#L310">AgilData</a>
     */
    private static int otherWare(int num_ware, int home_ware) {
        int tmp;
        if (num_ware == 1) return home_ware;
        while ((tmp = randomNumber(1, num_ware)) == home_ware);
        return tmp;
    }

}
