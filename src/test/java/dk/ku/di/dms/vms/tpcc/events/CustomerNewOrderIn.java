package dk.ku.di.dms.vms.tpcc.events;

import dk.ku.di.dms.vms.event.TransactionalEvent;

public final class CustomerNewOrderIn extends TransactionalEvent {

    public Integer c_w_id;
    public Integer c_d_id;
    public Integer c_id;

    public CustomerNewOrderIn(Integer c_w_id, Integer c_d_id, Integer c_id) {
        this.c_w_id = c_w_id;
        this.c_d_id = c_d_id;
        this.c_id = c_id;
    }
}
