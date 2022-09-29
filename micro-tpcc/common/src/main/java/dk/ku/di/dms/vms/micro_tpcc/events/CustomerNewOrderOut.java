package dk.ku.di.dms.vms.micro_tpcc.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public record CustomerNewOrderOut (
     float c_discount,
     String c_last,
     String c_credit,
    // simply forwarding
     int c_id) {}