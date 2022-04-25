package dk.ku.di.dms.vms.micro_tpcc.events;

import dk.ku.di.dms.vms.modb.common.event.IApplicationEvent;

public record ItemNewOrderIn(
        int[] itemsIds,
        int s_w_id)
        implements IApplicationEvent {}
