package dk.ku.di.dms.vms.micro_tpcc.events;

import dk.ku.di.dms.vms.modb.common.event.IVmsApplicationEvent;

public record ItemNewOrderOut(
        // Map<Integer,Float> itemsPrice
        int[] itemsIds,
        float itemPrices
) implements IVmsApplicationEvent {}
