package dk.ku.di.dms.vms.micro_tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public record StockNewOrderOut (
    int[] itemIds,
    String[] itemsDistInfo) {}