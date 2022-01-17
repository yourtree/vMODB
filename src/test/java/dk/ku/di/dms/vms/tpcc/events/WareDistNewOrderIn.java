package dk.ku.di.dms.vms.tpcc.events;

import dk.ku.di.dms.vms.event.TransactionalEvent;

public final class WareDistNewOrderIn extends TransactionalEvent {

    public Integer d_w_id;
    public Integer d_id;

}
