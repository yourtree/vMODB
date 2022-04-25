package dk.ku.di.dms.vms.micro_tpcc.events;

import dk.ku.di.dms.vms.modb.common.event.IApplicationEvent;

public record StockNewOrderIn(
     int[] itemsIds,
     int[] quantity,
     int[] supware,
     int ol_cnt) implements IApplicationEvent {}