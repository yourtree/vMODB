package dk.ku.di.dms.vms.tpcc.events;

import dk.ku.di.dms.vms.event.TransactionalEvent;
import java.util.List;

public final class ItemNewOrderIn extends TransactionalEvent {

    public List<Integer> itemsIds;
    public Integer s_w_id;

}
