package dk.ku.di.dms.vms.tpcc.events;

import dk.ku.di.dms.vms.event.TransactionalEvent;
import org.javatuples.Pair;

import java.util.Map;

public class StockNewOrderOut extends TransactionalEvent {
    public Map<Pair<Integer,Integer>,Float> itemsDistInfo;
}
