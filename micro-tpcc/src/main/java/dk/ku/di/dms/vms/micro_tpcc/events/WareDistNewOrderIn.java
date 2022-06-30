package dk.ku.di.dms.vms.micro_tpcc.events;

import dk.ku.di.dms.vms.modb.common.event.IVmsApplicationEvent;

public record WareDistNewOrderIn (
    int d_w_id,
    int d_id) implements IVmsApplicationEvent {}

