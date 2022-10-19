package dk.ku.di.dms.vms.micro_tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public record WareDistNewOrderIn (
    int d_w_id,
    int d_id) {}

