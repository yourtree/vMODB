package dk.ku.di.dms.vms.micro_tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public record CustomerNewOrderIn(
     int c_w_id,
     int c_d_id,
     int c_id) {}
