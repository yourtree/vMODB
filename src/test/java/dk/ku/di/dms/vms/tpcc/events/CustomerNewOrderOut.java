package dk.ku.di.dms.vms.tpcc.events;

import dk.ku.di.dms.vms.event.TransactionalEvent;

public class CustomerNewOrderOut extends TransactionalEvent {

    public Float c_discount;
    public String c_last;
    public String c_credit;
    // simply forwarding
    public Integer c_id;

}
