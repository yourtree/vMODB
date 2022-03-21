package dk.ku.di.dms.vms.tpcc.events;

import dk.ku.di.dms.vms.event.TransactionalEvent;
import dk.ku.di.dms.vms.utils.Pair;

import java.util.Map;

public class StockNewOrderOut extends TransactionalEvent {
    public Map<Pair<Integer,Integer>,String> itemsDistInfo;
}
