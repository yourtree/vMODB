package dk.ku.di.dms.vms.micro_tpcc.events;

import dk.ku.di.dms.vms.modb.common.event.IEvent;

public record WareDistNewOrderOut (
     float w_tax,
     float d_tax,
     int d_next_o_id,
     int d_w_id,
     int d_id) implements IEvent {}
