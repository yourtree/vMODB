package dk.ku.di.dms.vms.tpcc.events;

import dk.ku.di.dms.vms.event.TransactionalEvent;
import java.util.List;

public class StockNewOrderIn extends TransactionalEvent {

    public List<Integer> itemsIds;
    public List<Integer> quantity;
    public List<Integer> supware;
    public Integer ol_cnt;

}
