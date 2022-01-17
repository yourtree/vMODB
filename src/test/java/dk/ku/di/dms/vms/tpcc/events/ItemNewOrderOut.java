package dk.ku.di.dms.vms.tpcc.events;

import dk.ku.di.dms.vms.event.TransactionalEvent;
import java.util.Map;

public final class ItemNewOrderOut extends TransactionalEvent {

    public Map<Integer,Float> itemsPrice;

}
